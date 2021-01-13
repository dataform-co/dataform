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

std::unique_ptr<sandbox2::Policy> GetPolicy() {
  return sandbox2::PolicyBuilder()
      // The most frequent syscall should go first in this sequence (to make it
      // fast).
      // Allow read() with all arguments.
      .AllowRead()
      .AllowWrite()
      // Allow a preset of syscalls that are known to be used during startup
      // of static binaries.
      .AllowDynamicStartup()
      .AddLibrariesForBinary("/usr/local/google/home/eliaskassell/.cache/bazel/_bazel_eliaskassell/7a3b4b05af3e35677ea962500c529f6a/external/nodejs_linux_amd64/bin/nodejs/bin/node")
      .EnableNamespaces()
      // Allow the getpid() syscall.
      .AllowSyscall(__NR_getpid)

      // Suggestion to fix execveat error.
      .AddFileAt("/dev/zero", "/dev/fd/1022", false)

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
                          })
      // write() calls with fd not in (1, 2) will continue evaluating the
      // policy. This means that other rules might still allow them.

      // // Allow exit() only with an exit_code of 0.
      // // Explicitly jumping to KILL, thus the following rules can not
      // // override this rule.
      // .AddPolicyOnSyscall(
      //     __NR_exit_group,
      //     {// Load first argument (exit_code).
      //      ARG_32(0),
      //      // Deny every argument except 0.
      //      JNE32(0, KILL),
      //      // Allow all exit() calls that were not previously forbidden
      //      // = exit_code == 0.
      //      ALLOW})

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

  std::string workspaceFolder = "df/";
  std::string relativePath(argv[1]);
  const std::string nodePath = sandbox2::GetDataDependencyFilePath(workspaceFolder + relativePath);

  std::vector<std::string> args = {
        nodePath,
      };
  printf("Starting node bin from path '%s'\n", nodePath.c_str());
  auto executor = absl::make_unique<sandbox2::Executor>(nodePath, args);
  
  printf("Configuring executor settings\n");
  executor
    // Sandboxing is enabled by the sandbox itself. The sandboxed binary is
    // not aware that it'll be sandboxed.
    // Note: 'true' is the default setting for this class.
    ->set_enable_sandbox_before_exec(true)
    .limits()
    // Remove restrictions on the size of address-space of sandboxed
    // processes.
    ->set_rlimit_as(RLIM64_INFINITY);
  printf("Executor settings configured\n");

  int proc_version_fd = open("/proc/version", O_RDONLY);
  printf("Proc version: %i\n", proc_version_fd);
  PCHECK(proc_version_fd != -1);

  // Map this file's to sandboxee's stdin.
  printf("Mapping fd to stdin\n");
  executor->ipc()->MapFd(proc_version_fd, STDIN_FILENO);

  printf("Retrieving policy\n");
  auto policy = GetPolicy();
  printf("Applying policy\n");
  sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

  printf("Running sandbox\n");
  auto result = s2.Run();
  printf("Run complete\n");

  printf("Final execution status: %s\n", result.ToString().c_str());

  return result.final_status() == sandbox2::Result::OK ? EXIT_SUCCESS
                                                       : EXIT_FAILURE;
}
