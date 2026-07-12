# CI triplet: release-only dependency builds (halves gRPC build time and
# buildtree disk vs the default dbg+rel — GH runners have ~14GB free and the
# default triplet ran them out of space). Local dev keeps the default triplet.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_BUILD_TYPE release)
