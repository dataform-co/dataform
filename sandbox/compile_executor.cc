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
#include <glog/logging.h>
#include <linux/futex.h>
#include <linux/socket.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <syscall.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/internal/raw_logging.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "sandboxed_api/sandbox2/executor.h"
#include "sandboxed_api/sandbox2/limits.h"
#include "sandboxed_api/sandbox2/policy.h"
#include "sandboxed_api/sandbox2/policybuilder.h"
#include "sandboxed_api/sandbox2/result.h"
#include "sandboxed_api/sandbox2/sandbox2.h"
#include "sandboxed_api/sandbox2/util.h"
#include "sandboxed_api/sandbox2/util/bpf_helper.h"
#include "sandboxed_api/util/flag.h"
#include "sandboxed_api/util/runfiles.h"
#include "tools/cpp/runfiles/runfiles.h"

namespace fs = std::filesystem;

const int TIMEOUT_SECS = 1000;

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    std::string nodeRelativePath(argv[1]);
    std::string workerRelativeRoot(argv[2]);
    std::string socketPath(argv[3]);
    std::string compileConfigBase64(argv[4]);
    std::string projectDir(argv[5]);

    std::string currentPath = std::string(fs::current_path()) + "/";
    std::string nodePath =
        sapi::GetDataDependencyFilePath(nodeRelativePath);

    std::string workerRoot = sapi::GetDataDependencyFilePath(workerRelativeRoot);
    std::string workerBundle = workerRoot + "/worker_bundle.js";

    std::vector<std::string> args = {
        nodePath,
        "/worker_root/worker_bundle.js",
        socketPath,
        compileConfigBase64
    };

    auto executor = absl::make_unique<sandbox2::Executor>(nodePath, args);

    executor->set_enable_sandbox_before_exec(true)
        .limits()
        ->set_rlimit_as(RLIM64_INFINITY)
        .set_rlimit_fsize(4ULL << 20)
        .set_rlimit_cpu(RLIM64_INFINITY)
        .set_walltime_limit(absl::Seconds(90));

    int stdoutFd = executor->ipc()->ReceiveFd(STDOUT_FILENO);
    int stderrFd = executor->ipc()->ReceiveFd(STDERR_FILENO);

    auto policy = sandbox2::PolicyBuilder()
                      // Workaround to make the forkserver's execveat work.
                      .AddFileAt("/dev/zero", "/dev/fd/1022", false)

                      .AddFile(socketPath, false)
                      .AddDirectory(projectDir, true)
                      .AddLibrariesForBinary(nodePath)

                      .AddFileAt(workerRoot + "/worker_bundle.js", "/worker_root/worker_bundle.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/index.js", "/worker_root/node_modules/vm2/index.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/bridge.js", "/worker_root/node_modules/vm2/lib/bridge.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/builtin.js", "/worker_root/node_modules/vm2/lib/builtin.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/cli.js", "/worker_root/node_modules/vm2/lib/cli.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/compiler.js", "/worker_root/node_modules/vm2/lib/compiler.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/events.js", "/worker_root/node_modules/vm2/lib/events.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/filesystem.js", "/worker_root/node_modules/vm2/lib/filesystem.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/main.js", "/worker_root/node_modules/vm2/lib/main.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/nodevm.js", "/worker_root/node_modules/vm2/lib/nodevm.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/resolver-compat.js", "/worker_root/node_modules/vm2/lib/resolver-compat.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/resolver.js", "/worker_root/node_modules/vm2/lib/resolver.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/script.js", "/worker_root/node_modules/vm2/lib/script.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/setup-node-sandbox.js", "/worker_root/node_modules/vm2/lib/setup-node-sandbox.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/setup-sandbox.js", "/worker_root/node_modules/vm2/lib/setup-sandbox.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/transformer.js", "/worker_root/node_modules/vm2/lib/transformer.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/lib/vm.js", "/worker_root/node_modules/vm2/lib/vm.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/.bin/acorn", "/worker_root/node_modules/vm2/node_modules/.bin/acorn", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/acorn/bin/acorn", "/worker_root/node_modules/vm2/node_modules/acorn/bin/acorn", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/acorn/dist/acorn.js", "/worker_root/node_modules/vm2/node_modules/acorn/dist/acorn.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/acorn/dist/acorn.mjs", "/worker_root/node_modules/vm2/node_modules/acorn/dist/acorn.mjs", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/acorn/dist/bin.js", "/worker_root/node_modules/vm2/node_modules/acorn/dist/bin.js", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/node_modules/acorn/package.json", "/worker_root/node_modules/vm2/node_modules/acorn/package.json", true)
                      .AddFileAt(workerRoot + "/node_modules/vm2/package.json", "/worker_root/node_modules/vm2/package.json", true)
                      .AddFileAt(workerRoot + "/node_modules/acorn-walk/dist/walk.js", "/worker_root/node_modules/acorn-walk/dist/walk.js", true)
                      .AddFileAt(workerRoot + "/node_modules/acorn-walk/dist/walk.mjs", "/worker_root/node_modules/acorn-walk/dist/walk.mjs", true)
                      .AddFileAt(workerRoot + "/node_modules/acorn-walk/package.json", "/worker_root/node_modules/acorn-walk/package.json", true)

                      // System policies are described here as "[syscall number], reason".

                      // [202/futex], fast user-space locking, used by v8 when available.
                      // If not available, V8 will emulate them instead, which is slower:
                      // https://source.corp.google.com/cobalt/third_party/v8/src/execution/futex-emulation.h;rcl=8a873473f20e4e6ad0a507e6ae257e4d1bcc9416;l=22
                      .AllowFutexOp(FUTEX_WAKE)
                      .AllowFutexOp(FUTEX_WAIT)
                      .AllowFutexOp(FUTEX_CMP_REQUEUE)

                      // File and directory content handling.
                      .AllowRead()
                      .AllowReaddir()
                      .AllowWrite()
                      .AllowAccess()
                      .AllowGetIDs()

                      // [257/openat], open a file relative to a directory file descriptor.
                      // Required for opening files.
                      .AllowOpen()
                      // [9/mmap], map or unmap files or devices into memory.
                      // JS files are loaded into memory by V8.
                      .AllowMmap()

                      // [24/sched_yield], allow delegation back to the sandboxer on timeout.
                      .AllowSyscall(__NR_sched_yield)

                      // [302/prlimit64], set resource limits of 64 bit processes.
                      .AllowSyscall(__NR_prlimit64)

                      // Allow PKU for protecting spaces.
                      // https://groups.google.com/a/google.com/g/v8-google3/c/5qBIb3IQ4J0
                      .AllowSyscall(__NR_pkey_alloc)
                      .AllowSyscall(__NR_pkey_free)
                      .AllowSyscall(__NR_pkey_mprotect)

                      // [39/getpid], get process ID.
                      .AllowSyscalls({__NR_getpid, __NR_gettid})
                      // [56/clone], create a child process. Used for thread creation.
                      .AllowSyscall(__NR_clone)
                      // [234/tgkill], send a kill signal to a thread. Inparticular used when
                      // hitting memory limits.
                      .AllowSyscall(__NR_tgkill)
                      // Memory management.
                      .AllowTcMalloc()
                      // [28/madvise], give advice about use of memory
                      .AllowSyscall(__NR_madvise)
                      // [10/mprotect], set protection of a region of memory.
                      .AllowSyscall(__NR_mprotect)
                      // [324/membarrier], issue memory barriers.
                      .AllowSyscall(__NR_membarrier)
                      // [16/ioctl], used for terminal output.
                      .AllowSyscall(__NR_ioctl)
                      // [330/pkey_alloc] V8 uses for querying available memory protection.
                      .AllowSyscall(__NR_pkey_alloc)
                      // Needed in v8::base::Stack::GetStackStart().
                      .AllowSyscall(__NR_sched_getaffinity)
                      .AllowTime()
                      .AllowExit()
                      .AllowGetRandom()
                      .AllowDynamicStartup()
                      // For UDS communication.
                      .AllowSyscall(__NR_rt_sigprocmask)
                      .AllowSyscall(__NR_rt_sigaction)
                      .AllowSyscall(__NR_fcntl)
                      .AllowSyscall(__NR_getsockopt)
                      .AllowSyscall(__NR_setsockopt)
                      .AllowSyscall(__NR_sendto)
                      .AllowSyscall(__NR_shutdown)
                      .AllowSyscall(__NR_bind)
                      .AllowSyscall(__NR_listen)
                      .AllowSyscall(__NR_connect)
                      .AllowSyscall(__NR_getsockname)
                      .AllowSyscall(__NR_socket)
                      .AllowSyscall(__NR_socketpair)
                      .AllowSyscall(__NR_sendmmsg)
                      // Allow epoll I/O event notification and piping for fd data transferral.
                      .AllowSyscall(__NR_epoll_create1)
                      .AllowSyscall(__NR_epoll_ctl)
                      .AllowSyscall(__NR_epoll_wait)
                      .AllowSyscall(__NR_pipe2)
                      .AllowSyscall(__NR_eventfd2)

                      .AllowSyscall(435) // clone3
                      .AllowSyscall(__NR_sysinfo)
                      .AllowSyscall(__NR_statx)
                      .AllowSyscall(__NR_getcwd)
                      .BuildOrDie();

    sandbox2::Sandbox2 s2(std::move(executor), std::move(policy));

    // If the sandbox program fails to start, return early.
    if (!s2.RunAsync())
    {
        auto result = s2.AwaitResultWithTimeout(
            absl::Seconds(TIMEOUT_SECS));
        LOG(ERROR) << "sandbox failed to start: " << result->ToString();
        return EXIT_FAILURE;
    }

    auto result = s2.AwaitResultWithTimeout(
        absl::Seconds(TIMEOUT_SECS));

    printf("Final execution status: %s\n", result->ToString().c_str());

    return result.ok() && (result->final_status() == sandbox2::Result::OK) ? EXIT_SUCCESS
                                                          : EXIT_FAILURE;
}
