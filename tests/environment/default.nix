# A toy simulation environment ISO for the real-QEMU tests in
# tests/simulate_qemu.rs. It boots the stock nixpkgs kernel with a busybox
# initramfs, and init.sh implements the contract of a simulation environment.
#
# Build it with `nix-build tests/environment`. `mise run test:qemu` builds it and runs the tests.
{
  pkgs ? import ../../nixpkgs.nix { },
}:

let
  kernel = pkgs.linuxPackages.kernel;

  modules = pkgs.makeModulesClosure {
    kernel = kernel.modules;
    firmware = pkgs.emptyDirectory;
    rootModules = [
      "virtio_pci"
      "virtio_net"
      "virtio_blk"
      "erofs"
      "qemu_fw_cfg"
      # udhcpc uses a packet socket.
      "af_packet"
    ];
  };

  init = pkgs.replaceVarsWith {
    src = ./init.sh;
    isExecutable = true;
    replacements = {
      inherit (pkgs) bash busybox dropbear;
    };
  };

  initrd = pkgs.makeInitrd {
    compressor = "zstd";
    contents = [
      {
        object = init;
        symlink = "/init";
      }
      {
        object = "${modules}/lib/modules";
        symlink = "/lib/modules";
      }
    ];
  };
in
pkgs.runCommand "snouty-toy-environment.iso"
  {
    nativeBuildInputs = [ pkgs.xorriso ];
  }
  ''
    mkdir -p iso/isolinux
    cp ${pkgs.syslinux}/share/syslinux/{isolinux.bin,ldlinux.c32} iso/isolinux/
    cp ${kernel}/bzImage iso/isolinux/bzImage
    cp ${initrd}/initrd.zst iso/isolinux/initrd
    cat > iso/isolinux/isolinux.cfg <<'EOF'
    SERIAL 0 115200
    DEFAULT toy
    LABEL toy
      KERNEL bzImage
      INITRD initrd
      APPEND console=ttyS0 panic=-1
    EOF
    xorriso -as mkisofs -quiet -o $out \
      -b isolinux/isolinux.bin -c isolinux/boot.cat \
      -no-emul-boot -boot-load-size 4 -boot-info-table iso
  ''
