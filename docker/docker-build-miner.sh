# libraries
cp /usr/lib/x86_64-linux-gnu/libhwloc.so.15 .
cp /usr/lib/x86_64-linux-gnu/libOpenCL.so.1 .
#cp /lib/x86_64-linux-gnu/libhwloc.so.15 .
#cp /lib/x86_64-linux-gnu/libOpenCL.so.1 .
cp /lib/x86_64-linux-gnu/libgcc_s.so.1 .
cp /lib/x86_64-linux-gnu/libpthread.so.0 .
cp /lib/x86_64-linux-gnu/libm.so.6 .
cp /lib/x86_64-linux-gnu/libdl.so.2 .
cp /lib/x86_64-linux-gnu/libc.so.6 .
cp /lib/x86_64-linux-gnu/libudev.so.1 .
cp /usr/lib/x86_64-linux-gnu/libltdl.so.7 .
#cp /lib/x86_64-linux-gnu/libltdl.so.7 .

cp /opt/xilinx/xrt/lib/libxilinxopencl.so.2 .
cp /usr/local/IPFS/neptune_plus/libneptune_plus_ffi.so .
cp /opt/xilinx/xrt/lib/libxrt_coreutil.so.2 .
cp /usr/local/IPFS/libposeidon/libposeidon_hash.so .
cp /lib/x86_64-linux-gnu/libhwloc.so.5 .
cp /opt/xilinx/xrt/lib/libxrt_core.so.2 .
cp /lib/x86_64-linux-gnu/libnuma.so.1 .

# binary
cp ../lotus .
cp ../lotus-miner .

cp Dockerfile.miner Dockerfile

docker build -t octopus-fpga-seal-miner .

rm lotus-miner *.so.* Dockerfile
