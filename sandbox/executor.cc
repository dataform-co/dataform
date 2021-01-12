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

// Based on this example:
// https://github.com/google/sandboxed-api/blob/master/sandboxed_api/sandbox2/examples/static/static_sandbox.cc

#include <fcntl.h>
#include <sys/resource.h>
#include <syscall.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include "sandboxed_api/util/flag.h"
#include "absl/memory/memory.h"
#include "sandboxed_api/sandbox2/executor.h"
#include "sandboxed_api/sandbox2/limits.h"
#include "sandboxed_api/sandbox2/policy.h"
#include "sandboxed_api/sandbox2/policybuilder.h"
#include "sandboxed_api/sandbox2/result.h"
#include "sandboxed_api/sandbox2/sandbox2.h"
#include "sandboxed_api/sandbox2/util/bpf_helper.h"
#include "sandboxed_api/sandbox2/util/runfiles.h"

const std::string NODE_PATH = "/usr/bin/node";

std::unique_ptr<sandbox2::Policy> GetPolicy() {
  return sandbox2::PolicyBuilder()
      // The most frequent syscall should go first in this sequence (to make it
      // fast).
      // Allow read() with all arguments.
      .AllowRead()
      .AllowWrite()
      // Allow a preset of syscalls that are known to be used during startup
      // of static binaries.
      .AllowStaticStartup()
      .EnableNamespaces()
      .AddFile(NODE_PATH)
      // Allow the getpid() syscall.
      .AllowSyscall(__NR_getpid)

// #ifdef __NR_access
//       // On Debian, even static binaries check existence of /etc/ld.so.nohwcap.
//       .BlockSyscallWithErrno(__NR_access, ENOENT)
// #endif

      // Examples for AddPolicyOnSyscall:
      .AddPolicyOnSyscall(__NR_write,
                          {
                              // Load the first argument of write() (= fd)
                              ARG_32(0),
                              // Allow write(fd=STDOUT)
                              JEQ32(1, ALLOW),
                              // Allow write(fd=STDERR)
                              JEQ32(2, ALLOW),
                              JEQ32(3, ALLOW),
                              JEQ32(4, ALLOW),
                          })
      // write() calls with fd not in (1, 2) will continue evaluating the
      // policy. This means that other rules might still allow them.

      // Allow exit() only with an exit_code of 0.
      // Explicitly jumping to KILL, thus the following rules can not
      // override this rule.
      .AddPolicyOnSyscall(
          __NR_exit_group,
          {// Load first argument (exit_code).
           ARG_32(0),
           // Deny every argument except 0.
           JNE32(0, KILL),
           // Allow all exit() calls that were not previously forbidden
           // = exit_code == 0.
           ALLOW})

      // = This won't have any effect as we handled every case of this syscall
      // in the previous rule.
      .AllowSyscall(__NR_exit_group)

#ifdef __NR_open
      .BlockSyscallWithErrno(__NR_open, ENOENT)
#else
      .BlockSyscallWithErrno(__NR_openat, ENOENT)
#endif
      .BuildOrDie();
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  std::vector<std::string> args = {
        NODE_PATH,
      //   "/usr/local/google/home/eliaskassell/Documents/github/dataform/tmp.js"
      };
  auto executor = absl::make_unique<sandbox2::Executor>(NODE_PATH, args);
  
  executor
    // Sandboxing is enabled by the sandbox itself. The sandboxed binary is
    // not aware that it'll be sandboxed.
    // Note: 'true' is the default setting for this class.
    ->set_enable_sandbox_before_exec(true)
    .limits()
    // Remove restrictions on the size of address-space of sandboxed
    // processes.
    ->set_rlimit_as(RLIM64_INFINITY)
    // Kill sandboxed processes with a signal (SIGXFSZ) if it writes more than
    // these many bytes to the file-system.
    .set_rlimit_fsize(1024 * 1024)
    // The CPU time limit.
    .set_rlimit_cpu(60)
    .set_walltime_limit(absl::Seconds(30));

  int proc_version_fd = open("/proc/version", O_RDONLY);
  printf("Proc version: %i", proc_version_fd);
  PCHECK(proc_version_fd != -1);

  // Map this file's to sandboxee's stdin.
  executor->ipc()->MapFd(proc_version_fd, STDIN_FILENO);

  auto policy = GetPolicy();
  sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

  printf("Running sandbox");
  auto result = s2.Run();
  printf("Run complete");

  LOG(INFO) << "Final execution status: " << result.ToString();

  return result.final_status() == sandbox2::Result::OK ? EXIT_SUCCESS
                                                       : EXIT_FAILURE;
}
