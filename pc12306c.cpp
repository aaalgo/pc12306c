#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <sys/time.h>
#include <iostream>
#include <future>
#include <string>
#include <vector>
#include <stdexcept>
#include <random>
#include <chrono>
#include <thread>
#include <boost/program_options.hpp>
#include <boost/timer/timer.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/moment.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#ifdef _OPENMP
#include <omp.h>
#endif

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;
using std::future;
using std::async;
using std::launch;
using std::runtime_error;
using std::default_random_engine;
using boost::timer::auto_cpu_timer;

namespace ba = boost::accumulators;
typedef ba::accumulator_set<double, ba::stats<ba::tag::mean, ba::tag::min, ba::tag::max, ba::tag::count, ba::tag::variance, ba::tag::moment<2>>> Acc;

struct __attribute__ ((__packed__))  NetReq {
    int64_t        reqID;
    int32_t        train;        // [0, 5000)
    int16_t        start;        // [0, 10)
    int16_t        length;    // [1, 10]
};

struct __attribute__ ((__packed__))  NetResp {
    int64_t        reqID;
    int32_t        respID;
    int32_t        seat;
};

struct NetStat {
    struct timeval req_ts;
    struct timeval resp_ts;
    NetReq const *req;
    NetResp const *resp;

    NetStat (): req(nullptr), resp(nullptr) {
    }

    double latency () const {
        double ss = resp_ts.tv_sec - req_ts.tv_sec;
        ss += (resp_ts.tv_usec - req_ts.tv_usec) / 1000000.0;
        return ss;
    }
};

struct TicketSpace {
    int trains;
    int segments;
    int seats;
    int max_length;

    // generate one random request
    template <typename RANDOM_ENGINE>
    void sample (RANDOM_ENGINE &e, NetReq *req) const {
        req->train = e() % trains;
        req->start = e() % segments;
        int ml = segments - req->start;
        // extreme case, last segment:  req->start = segments - 1
        // ml = 1
        if (ml > max_length) {
            ml = max_length;
        }
        req->length = (e() % ml) + 1;
    }
};

class Client: public vector<NetStat> {

    TicketSpace ts;
    string server;
    short port;
    int sockfd;
    bool done;  // stop flag, reader/writer will
                // stop when done becomes true
    size_t n_send;
    size_t n_recv;
    future<void> rfuture;
    future<void> wfuture;

    vector<NetReq> reqs;
    vector<NetResp> resps;

    void reader () {
        for (auto &resp: resps) {
            if (done) break;
            struct timeval tv;
            ssize_t sz = recv(sockfd, reinterpret_cast<char *>(&resp), sizeof(resp), 0);
            gettimeofday(&tv, NULL);
            if (sz != sizeof(resp)) throw runtime_error("error resp");
            if (resp.reqID < 0 || resp.reqID >= size()) {
                throw runtime_error("bad server response");
            }
            auto &st = at(resp.reqID);
            st.resp = &resp;
            st.resp_ts = tv;
            ++n_recv;
        }
    }

    void writer () {
        for (unsigned i = 0; i < reqs.size(); ++i) {
            if (done) break;
            auto const &req = reqs[i];
            auto &st = at(i);
            st.req = &req;
            gettimeofday(&st.req_ts, NULL);
            ssize_t sz = send(sockfd, reinterpret_cast<char const *>(&req), sizeof(req), 0);
            if (sz != sizeof(req)) throw runtime_error("error sending");
            ++n_send;
        }
        done = true;
    }


public:
    Client ()
        : sockfd(-1), done(false), n_send(0), n_recv(0)
    {
    }

    ~Client () {
        BOOST_VERIFY(sockfd < 0);
    }

    // pre-generate random queries
    template <typename RANDOM_ENGINE>
    void generate (TicketSpace ts, size_t n, RANDOM_ENGINE &e) {
        reqs.resize(n);
        resps.resize(n);
        resize(n);
        for (unsigned i = 0; i < reqs.size(); ++i) {
            auto &req = reqs[i];
            req.reqID = i;
            ts.sample(e, &req);
        }
    }

    void start (string const &server, short port) {
        struct sockaddr_in serv_addr;
        struct hostent *ent;

        bzero((char *) &serv_addr, sizeof(serv_addr));

        ent = gethostbyname(server.c_str());
        if (ent == NULL) {
            throw runtime_error("ERROR, no such host");
        }
        serv_addr.sin_family = AF_INET;
        bcopy((char *)ent->h_addr, 
             (char *)&serv_addr.sin_addr.s_addr,
             ent->h_length);
        serv_addr.sin_port = port;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) 
            throw runtime_error("ERROR opening socket");
        if (::connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) 
            throw runtime_error("ERROR connecting");
        rfuture = async(launch::async, &Client::reader, this);
        wfuture = async(launch::async, &Client::writer, this);
    }

    void stop () { done = true;
    }

    void join () {
        rfuture.get();
        wfuture.get();
        close(sockfd);
        sockfd = -1;
    }

    size_t getSendCount () const {
        return n_send;
    }

    size_t getRecvCount () const {
        return n_recv;
    }
};

void count_send_recv (vector<Client> const *clients, size_t *n_send, size_t *n_recv) {
    size_t s = 0, r = 0;
    for (auto const &c: *clients) {
        s += c.getSendCount();
        r += c.getRecvCount();
    }
    *n_send = s;
    *n_recv = r;
}

void monitor_throughput (vector<Client> const *clients, float cycle, bool *stop) {

    size_t o_send = 0, o_recv = 0;
    count_send_recv(clients, &o_send, &o_recv);
    std::chrono::milliseconds delta(int(cycle * 1000));
    auto next = std::chrono::system_clock::now() + delta;
    while (!*stop) {
        size_t n_send, n_recv;
        std::this_thread::sleep_until(next);
        next += delta;
        count_send_recv(clients, &n_send, &n_recv);
        cerr << "send: " << (n_send - o_send)
             << " recv: " << (n_recv - o_recv)
             << endl;
        o_send = n_send;
        o_recv = n_recv;
    }
}

class Signal {
    static vector<Client> *clients;
    static void stop_all (int) {
        if (clients) {
            for (auto &client: *clients) {
                client.stop();
            }
        }
    }
public:
    Signal (vector<Client> *clients_) {
        BOOST_VERIFY(clients == nullptr);
        clients = clients_;
        signal(SIGTERM, &Signal::stop_all);
        signal(SIGINT, &Signal::stop_all);
    }

    ~Signal () {
        clients = nullptr;
    }
};

vector<Client> *Signal::clients = nullptr;

int main (int argc, char *argv[]) {

    namespace po = boost::program_options; 
    TicketSpace tspace;
    string server;
    short port;
    size_t N, T;
    float cycle;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("server,s", po::value(&server)->default_value("localhost"), "")
    ("port,p", po::value(&port)->default_value(12306), "")
    ("trains", po::value(&tspace.trains)->default_value(5000), "")
    ("segments", po::value(&tspace.segments)->default_value(10), "")
    ("seats", po::value(&tspace.seats)->default_value(3000), "")
    ("max-length", po::value(&tspace.max_length)->default_value(5), "")
    (",N", po::value(&N)->default_value(100000000), "queries per client")
    (",T", po::value(&T)->default_value(2), "number parallel clients")
    ("cycle", po::value(&cycle)->default_value(1), "throughput print cycle in seconds")
    ("help", "")
    ;

    po::positional_options_description p;

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help")) {
        cerr << desc;
        return 1;
    }
    
    vector<Client> clients(T);
    {
        auto_cpu_timer timer(cerr);
        cerr << "Generating queries..." << endl;
#pragma omp parallel
        {
            int seed = 0;
#ifdef _OPENMP
            seed = omp_get_thread_num();
#endif
            default_random_engine engine(seed);
#pragma omp for
            for (unsigned i = 0; i < clients.size(); ++i) {
                clients[i].generate(tspace, N, engine);
            }
        }
    }

    cerr << "Starting clients..." << endl;
    Signal signal(&clients);
    for (auto &client: clients) {
        client.start(server, port);
    }
    bool stop_monitor = false;
    asm volatile("": : :"memory");
    future<void> ft = async(launch::async, monitor_throughput, &clients, cycle, &stop_monitor);
    // wait for clients
    for (auto &client: clients) {
        client.join();
    }
    stop_monitor = true;
    asm volatile("": : :"memory");
    // wait for throughput monitor
    ft.get();

    // do statistics
    Acc acc;
    for (auto const &client: clients) {
        for (NetStat const &st: client) {
            if (!st.req) continue;
            if (!st.resp) continue;
            acc(st.latency());
        }
    }
    cout << "mean: " << ba::mean(acc) << endl;
    cout << "std: " << sqrt(ba::variance(acc)) << endl;
    cout << "min: " << ba::min(acc) << endl;
    cout << "max: " << ba::max(acc) << endl;
    cout << "count: " << ba::count(acc) << endl;

    return 0;
}
