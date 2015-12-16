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
#include <array>
#include <vector>
#include <stdexcept>
#include <random>
#include <chrono>
#include <thread>
#include <limits>
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
#include <parallel/algorithm>
#ifdef _OPENMP
#include <omp.h>
#endif

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::array;
using std::vector;
using std::future;
using std::async;
using std::launch;
using std::runtime_error;
using std::numeric_limits;
using std::default_random_engine;
using boost::timer::auto_cpu_timer;

namespace ba = boost::accumulators;
typedef ba::accumulator_set<double, ba::stats<ba::tag::mean, ba::tag::min, ba::tag::max, ba::tag::count, ba::tag::variance, ba::tag::moment<2>>> Acc;

struct __attribute__ ((__packed__))  NetReq {
    int64_t        reqID;
    int32_t        train;        // [0, 5000)
    int16_t        start;        // [0, 10)
    int16_t        stop;
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
    int sid;    // global seat id, == (train * tspace.seats + resp.seat) * segments + segment

    NetStat (): req(nullptr), resp(nullptr), sid(-1) {
    }

    // in seconds
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

    void check_size () {
        int64_t v = 1;
        v *= trains;
        v *= segments;
        v *= max_length;
        BOOST_VERIFY(v <= numeric_limits<int>::max());
    }

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
        int length = (e() % ml) + 1;
        req->stop = req->start + length;
    }

    // SID uniquely identify (train, seat, segment) triplet
    int make_sid (int train, int seat, int segment) const {
        return (train * seats + seat) * segments + segment;
    }

    int total_sids () const {
        return trains * seats * segments;
    }
};

enum {  // performance counters
    CNT_SEND = 0,
    CNT_RECV,
    CNT_SUCC,
    CNT_NUM
};

typedef array<size_t, CNT_NUM> Counters;

class Client: public vector<NetStat> {

    TicketSpace ts;
    int sockfd;
    bool volatile done;  // stop flag, reader/writer will
                // stop when done becomes true
    size_t batch;
    size_t queue;
    size_t sleep;
    Counters counters;
    size_t volatile *pcounters;

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
            ++pcounters[CNT_RECV];
            if (resp.seat >= 0) {
                ++pcounters[CNT_SUCC];
                st.sid = ts.make_sid(st.req->train, resp.seat, st.req->start);
            }
        }
    }

    void writer () {
        for (unsigned i = 0; i < reqs.size(); i += batch) {
            while ((pcounters[CNT_SEND] + batch - pcounters[CNT_RECV] >= queue) && (!done)) {
                usleep(sleep);
            }
            // i is batch start
            if (done) break;
            struct timeval ts;
            gettimeofday(&ts, NULL);
            unsigned n = reqs.size() - i;
            if (n > batch) n = batch;
            for (unsigned j = i; j < i + n; ++j) {
                auto const &req = reqs[j];
                auto &st = at(j);
                st.req_ts = ts;
                st.req = &req;
            }
            ssize_t sz = send(sockfd, reinterpret_cast<char const *>(&reqs[i]), sizeof(NetReq) * n, 0);
            if (sz != sizeof(NetReq) * n) throw runtime_error("error sending");
            pcounters[CNT_SEND] += batch;
        }
        done = true;
    }


public:
    Client ()
        : sockfd(-1), done(false), batch(1), queue(100), sleep(100), pcounters(&counters[0])
    {
        counters.fill(0);
    }

    ~Client () {
        BOOST_VERIFY(sockfd < 0);
    }

    void setBatch (size_t b) {
        batch = b;
    }

    void setQueue (size_t q) {
        queue = q;
    }

    void setSleep (size_t s) {
        sleep = s;
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

    void start (string const &server, unsigned short port) {
        struct sockaddr_in serv_addr;
        struct hostent *ent;

        BOOST_VERIFY(batch < queue);

        bzero((char *) &serv_addr, sizeof(serv_addr));

        ent = gethostbyname(server.c_str());
        if (ent == NULL) {
            throw runtime_error("ERROR, no such host");
        }
        serv_addr.sin_family = AF_INET;
        bcopy((char *)ent->h_addr, 
             (char *)&serv_addr.sin_addr.s_addr,
             ent->h_length);
        serv_addr.sin_port = htons(port);

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

    void getCounters (Counters *c) const {
        *c = counters;
    }
};

void count_all (vector<Client> const &clients, Counters *p) {
    Counters total;
    total.fill(0);
    for (auto const &c: clients) {
        Counters cnts;
        c.getCounters(&cnts);
        for (unsigned i = 0; i < total.size(); ++i) {
            total[i] += cnts[i];
        }
    }
    *p = total;
}

// print performance counters every cycle seconds
// stop when *stop becomes true
void monitor_throughput (vector<Client> const *clients, float cycle, bool *stop) {
    Counters old;
    count_all(*clients, &old);
    std::chrono::milliseconds delta(int(cycle * 1000));
    auto next = std::chrono::system_clock::now() + delta;
    while (!*stop) {
        std::this_thread::sleep_until(next);
        next += delta;
        Counters cnts;
        count_all(*clients, &cnts);
        cerr << "send: " << (cnts[CNT_SEND] - old[CNT_SEND])
             << " recv: " << (cnts[CNT_RECV] - old[CNT_RECV])
             << " succ: " << (cnts[CNT_SUCC] - old[CNT_SUCC])
             << endl;
        old = cnts;
    }
}

// MUST BE SINGLETON
// handle signals during the life cycle of this object
// when signal received, stop clients
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
        signal(SIGTERM, SIG_DFL);
        signal(SIGINT, SIG_DFL);
    }
};

vector<Client> *Signal::clients = nullptr;

int main (int argc, char *argv[]) {

    namespace po = boost::program_options; 
    TicketSpace tspace;
    string server;
    unsigned short port;
    size_t N, T, B, Q, S;
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
    (",N", po::value(&N)->default_value(10000000), "queries per client")
    (",T", po::value(&T)->default_value(2), "number parallel clients")
    (",B", po::value(&B)->default_value(1), "request batch size")
    (",Q", po::value(&Q)->default_value(1000), "")
    (",S", po::value(&S)->default_value(100), "if queue is fall, sleep this # us")
    ("cycle", po::value(&cycle)->default_value(1), "print counters every cycle seconds")
    ;

    po::positional_options_description p;

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help")) {
        cerr << desc;
        return 0;
    }

    tspace.check_size();
    
    vector<Client> clients(T);
    {
        auto_cpu_timer timer(cerr);
        cerr << "Generating queries..." << endl;
        // this is not efficient when # clients < # available OMP threads
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

    {
        cerr << "Starting clients, interrupt with Ctrl+C..." << endl;
        Signal signal(&clients);
        auto_cpu_timer timer(cerr);
        for (auto &client: clients) {
            client.setBatch(B);
            client.setQueue(Q);
            client.start(server, port);
        }
        bool stop_monitor = false;
        future<void> ft = async(launch::async, monitor_throughput, &clients, cycle, &stop_monitor);
        // wait for clients
        for (auto &client: clients) {
            client.join();
        }
        if (clients.empty()) {
            cerr << "No clients running, sleep 10s for testing..." << endl;
            sleep(10); // if no clients, sleep 00s for testing.
        }
        stop_monitor = true;
        asm volatile("": : :"memory");
        // wait for throughput monitor
        ft.get();
    }

    // do statistics
    Counters cnts;
    count_all(clients, &cnts);
    cerr << "send: " << cnts[CNT_SEND] << endl;
    cerr << "received: " << cnts[CNT_RECV] << endl;
    cerr << "success: " << cnts[CNT_SUCC] << endl;
    Acc acc;
    for (auto const &client: clients) {
        for (NetStat const &st: client) {
            if (!st.req) continue;
            if (!st.resp) continue;
            acc(st.latency()); // in seconds
        }
    }
    cout << "latency.mean: " << ba::mean(acc) * 1000 << " ms" << endl;
    cout << "latency.std: " << sqrt(ba::variance(acc)) * 1000 << " ms" << endl;
    cout << "latency.min: " << ba::min(acc) * 1000  << " ms" << endl;
    cout << "latency.max: " << ba::max(acc) * 1000 << " ms" << endl;
    cout << "latency.count: " << ba::count(acc) << endl;
    // the array of tickets assigned to each seat
    vector<NetStat const *> v;
    size_t asked = 0;
    size_t sold = 0;
    //v.reserve(tspace.total_sids());
    for (auto const &client: clients) {
        for (auto const &st: client) {
            if (st.req) {
                ++asked;
                if (st.sid >= 0) {
                    v.push_back(&st);
                    sold += st.req->stop - st.req->start;
                }
            }
        }
    }
    cout << "fill.rate: " <<  1.0 * sold / tspace.total_sids() << "    (rate of inventory sold)" << endl;
    cout << "fulfill.rate: " <<  1.0 * v.size() / asked << "     (rate of reqs satisfied)" << endl;
    // detect conflicts
    __gnu_parallel::sort(v.begin(), v.end(),
                [](NetStat const *p1, NetStat const *p2) {
                    return p1->sid < p2->sid;
                });
    // verify no overlap
    size_t conflict = 0;
    for (unsigned i = 1; i < v.size(); ++i) {
        if (v[i-1]->req->train != v[i]->req->train) continue;
        if (v[i-1]->resp->seat != v[i]->resp->seat) continue;
        // same seat on same train, detect conflict
        if (v[i-1]->req->stop >= v[i]->req->start) {
            /*
            cout << "conflict: "
                << v[i-1]->resp->reqID << '.'
                << v[i-1]->resp->respID
                << " and "
                << v[i]->resp->reqID << '.'
                << v[i]->resp->respID
                << endl;
            */
            ++conflict;
        }
    }
    cout << "conflict: " << conflict << endl;

    return 0;
}
