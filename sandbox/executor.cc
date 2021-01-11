// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <syscall.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include "sandboxed_api/util/flag.h"
#include "absl/memory/memory.h"
#include "sandboxed_api/sandbox2/comms.h"
#include "sandboxed_api/sandbox2/executor.h"
#include "sandboxed_api/sandbox2/forkserver.h"
#include "sandboxed_api/sandbox2/limits.h"
#include "sandboxed_api/sandbox2/policy.h"
#include "sandboxed_api/sandbox2/policybuilder.h"
#include "sandboxed_api/sandbox2/result.h"
#include "sandboxed_api/sandbox2/sandbox2.h"
#include "sandboxed_api/sandbox2/util/runfiles.h"

std::unique_ptr<sandbox2::Policy> GetPolicy() {
  return sandbox2::PolicyBuilder()
      // The most frequent syscall should go first in this sequence (to make it
      // fast).
      .AllowRead()
      .AllowWrite()
      .AllowExit()
      .AllowTime()
      .EnableNamespaces()
      .AllowSyscalls({
        __NR_close, __NR_getpid,
#if defined(__NR_arch_prctl)
            // Not defined with every CPU architecture in Prod.
            __NR_arch_prctl,
#endif  // defined(__NR_arch_prctl)
      })
#if defined(ADDRESS_SANITIZER) || defined(MEMORY_SANITIZER) || \
    defined(THREAD_SANITIZER)
      .AllowMmap()
#endif
      .BuildOrDie();
}

bool RunNodeScript(sandbox2::Comms* comms, std::string script, std::string* response) {
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(script.data());
  size_t buf_size = script.size();

  if (!comms->SendBytes(buf, buf_size)) {
    LOG(ERROR) << "RunNodeScript_comms->SendBytes() failed";
    return false;
  }

  if (!comms->RecvString(response)) {
    LOG(ERROR) << "RunNodeScript_comms->RecvString failed";
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  const std::string path = "/usr/bin/node";
  std::vector<std::string> args = {path, "/usr/local/google/home/eliaskassell/Documents/github/dataform/tmp.js"};
  std::vector<std::string> envs = {};
  auto executor = absl::make_unique<sandbox2::Executor>(path, args, envs);
  
  executor->set_enable_sandbox_before_exec(true);

  auto* comms = executor->ipc()->comms();
  auto policy = GetPolicy();

  sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

  // Let the sandboxee run.
  if (!s2.RunAsync()) {
    auto result = s2.AwaitResult();
    LOG(ERROR) << "RunAsync failed: " << result.ToString();
    return 2;
  }
  
  std::string script = "console.log('HeLlO tHeRe')";
  std::string response;

  // if (!RunNodeScript(comms, script, &response)) {
  //   LOG(ERROR) << "Sending failed";
  //   if (!s2.IsTerminated()) {
  //     // Kill the sandboxee, because failure to receive the data over the Comms
  //     // channel doesn't automatically mean that the sandboxee itself had
  //     // already finished. The final reason will not be overwritten, so if
  //     // sandboxee finished because of e.g. timeout, the TIMEOUT reason will be
  //     // reported.
  //     LOG(INFO) << "Killing sandboxee";
  //     s2.Kill();
  //   }
  // }

  auto result = s2.AwaitResult();
  if (result.final_status() != sandbox2::Result::OK) {
    LOG(ERROR) << "Sandbox error: " << result.ToString();
    return 3;  // e.g. sandbox violation, signal (sigsegv)
  }
  auto code = result.reason_code();
  if (code) {
    LOG(ERROR) << "Sandboxee exited with non-zero: " << code;
    return 4;  // e.g. normal child error
  }
  LOG(INFO) << "Sandboxee finished: " << result.ToString();
  printf("RESPONSE: %s\n", response.c_str());
  return EXIT_SUCCESS;
}
