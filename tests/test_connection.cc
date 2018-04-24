#include "ps/ps.h"

int main(int argc, char *argv[]) {

  ps::Start(0);
  //Postoffice::Postoffice() // KAFKAVan() { }
  //Postoffice::Start() // van_->Start(customer_id);
  // Barrier()
  // do nothing
  printf("connect OK");
  ps::Finalize(0, true);

  return 0;
}
