echo "export FFI_BUILD_FROM_SOURCE=1;export FFI_USE_BLST=1;export FFI_USE_GPU=1;export FFI_USE_MULTICORE_SDR=1;export FFI_USE_GPU=1;export FFI_BUILD_FROM_SOURCE=1;export RUSTFLAGS="-C target-cpu=znver2 -g";export PATH=$PATH:/usr/local/go/bin;export FFI_USE_GPU2=1"

echo "cd /home/reaper/workspace/octopus_seal"
echo "cd extern/filecoin-ffi/rust"
echo "ssh-agent zsh"
echo "ssh-add ~/.ssh/id_rsa"
echo "cargo update"
echo "cd /home/reaper/workspace/octopus_seal"

echo "export FFI_BUILD_FROM_SOURCE=1;export FFI_USE_BLST=1;export FFI_USE_GPU=1;export FFI_USE_MULTICORE_SDR=1;export FFI_USE_GPU=1;export FFI_BUILD_FROM_SOURCE=1;export RUSTFLAGS=\"-C target-cpu=znver2 -g\";export PATH=$PATH:/usr/local/go/bin;export FFI_USE_GPU2=1"
echo "make clean calibnet"
