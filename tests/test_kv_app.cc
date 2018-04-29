#include "ps/ps.h"
using namespace ps;

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker() {
  if (!IsWorker()) return;
  KVWorker<float> kv(0, 0);

  // init
  int num = 1;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);
  //std::vector<int> lens(num);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + rank;//%llu
    vals[i] = (rand() % 1000);
    //lens[i] = 1;
  }


  // push
  int repeat = 1;
  std::vector<int> ts;
  for (int i = 0; i < repeat; ++i) {
    ts.push_back(kv.Push(keys, vals));//kv.Push(keys, vals, lens)

    // to avoid too frequency push, which leads huge memory usage
    if (i > 10) kv.Wait(ts[ts.size()-10]);
  }
  for (int t : ts) kv.Wait(t);

  // pull
  std::vector<float> rets;
  kv.Wait(kv.Pull(keys, &rets));//kv.Pull(keys, &rets, &lens)

  float res = 0;
  for (int i = 0; i < num; ++i) {
    res += fabs(rets[i] - vals[i] * repeat);
  }
  CHECK_LT(res / repeat, 1e-5);
  LL << "error: " << res / repeat;
}

int main(int argc, char *argv[]) {

  // setup server nodes
  StartServer();
  // start system
  Start(0);
  // run worker nodes
  RunWorker();
  // stop system
  Finalize(0, true);
  printf("kv_app OK\n");
  return 0;

}
