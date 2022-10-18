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

# keys
cp ~/.ssh/id_rsa.pub authorized_keys 

# binary
cp ../lotus .

cp Dockerfile.daemon Dockerfile

docker build -t octopus-daemon .

rm lotus authorized_keys *.so.* Dockerfile
