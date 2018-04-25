#include "ps/ps.h"
using namespace ps;

int num = 0;

void ReqHandle(const SimpleData& req, SimpleApp* app) {
  CHECK_EQ(req.head, 1);
  CHECK_EQ(req.body, "test");
  app->Response(req);
  ++ num;
}

int main(int argc, char *argv[]) {

  int n = 100;
  SimpleApp app(0, 0);
  app.set_request_handle(ReqHandle);

  Start(0);
  if (IsScheduler()) {
    std::vector<int> ts;
    for (int i = 0; i < n; ++i) {
      int recver = kScheduler + kServerGroup + kWorkerGroup;
      ts.push_back(app.Request(1, "test", recver));
    }

    for (int t : ts) {
      app.Wait(t);
    }
  }

  Finalize(0, true);

  CHECK_EQ(num, n);
  printf("simple app OK\n");
  return 0;
}
