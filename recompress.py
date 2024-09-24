import subprocess
import hashlib
import random
import shlex
import time
import sys
import os


try:
    import blake3

    DIGESTCLASS = blake3.blake3
except ImportError:
    import hashlib

    DIGESTCLASS = hashlib.sha512

print(f"using {DIGESTCLASS} as the hashing algorithm")


def pretty_filesize(fsize: int) -> str:
    if fsize == 1:
        return "1 Byte"
    if fsize < 1024:
        return f"{fsize} Bytes"
    if fsize < 1024 * 1024:
        return f"{fsize / 1024:.1f} KiB"
    if fsize < 1024 * 1024 * 1024:
        return f"{fsize / (1024 * 1024):.1f} MiB"
    return f"{fsize / (1024 * 1024 * 1024):.1f} GiB"


def getdigest(fname: str) -> str:
    args = ["zstd", "-d", "--stdout", fname]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as job:
        # NOTE: not using hashlib.file_digest since it's only in 3.11+
        digest = DIGESTCLASS()
        buffer = bytearray(64 * 1024)
        memview = memoryview(buffer)
        while True:
            readc = job.stdout.readinto(buffer)
            if readc == 0:
                break
            digest.update(memview[:readc])
        return digest.hexdigest()


def check_hashes(rawdigest: str, fname1: str, fname2: str) -> bool:
    h1 = getdigest(fname1)
    h2 = getdigest(fname2)
    okay = True
    if rawdigest != h1:
        okay = False
        print(f"raw data hash and hash of {fname1} didn't match - {rawdigest} vs. {h1}")
    if rawdigest != h2:
        okay = False
        print(f"raw data hash and hash of {fname2} didn't match - {rawdigest} vs. {h2}")
    if h1 != h2:
        okay = False
        print(f"hashes of {fname1} vs {fname2} didn't match - {h1} vs. {h2}")
    return okay


def create_temp_filename(origfname: str) -> str:
    dpath = os.path.split(origfname)[0]
    root = os.path.splitext(origfname)[0]
    for _ in range(10 * 1000):  # 10 thousand attempts
        items = (random.randint(0, 10**9), os.getpid(), origfname, time.time())
        xx = DIGESTCLASS(str(items).encode("UTF-8")).hexdigest()[:40]
        randompart = f".{xx}.temp"
        ret = os.path.join(dpath, root + randompart + ".zst")
        if os.path.exists(ret):
            continue
        return ret
    raise RuntimeError("failed to create a temporary filename")


def create_goal_filename(origfname: str) -> str:
    dpath = os.path.split(origfname)[0]
    root = os.path.splitext(origfname)[0]
    return os.path.join(dpath, root + ".zst")


def extract_argument(args: list, arg: str) -> int:
    ret = args.count(arg)
    while arg in args:
        args.remove(arg)
    return ret


def myshlexjoin(parts) -> str:
    """Same as shlex.join from 3.8+ using only shlex.quote from Python 3.3+"""
    return " ".join(map(shlex.quote, parts))


def main():
    args = sys.argv[1:]

    rm = extract_argument(args, "--rm")
    if len(args) != 1:
        print(f"not exactly one argument left among: {repr(args)}")
        sys.exit(1)

    gzfname = args[0]

    original_extension = os.path.splitext(gzfname)[1]

    KNOWN_EXTENSIONS = (".gz",)

    if original_extension not in KNOWN_EXTENSIONS:
        print(f"unknown extension: {original_extension}")
        sys.exit(1)

    finalname = create_goal_filename(gzfname)
    if os.path.exists(finalname):
        print(f"{finalname} already exists - quitting.")
        sys.exit(1)

    tempfname = create_temp_filename(gzfname)

    start_time = time.time()

    args1 = ["zstd", "-d", "--stdout", gzfname]
    gzipjob = subprocess.Popen(args1, stdout=subprocess.PIPE)

    args2 = ["zstd", "--quiet", "-o", tempfname]
    zstdjob = subprocess.Popen(args2, stdin=subprocess.PIPE)

    print(f"running: {myshlexjoin(args1)} | {myshlexjoin(args2)}")

    digest = DIGESTCLASS()
    rawsize = 0
    buffer = bytearray(64 * 1024)
    memview = memoryview(buffer)
    while True:
        readc = gzipjob.stdout.readinto(buffer)
        if readc == 0:
            zstdjob.stdin.close()  # need to close stdin to make zstd process end
            break

        rawsize += readc
        mv = memview[:readc]
        zstdjob.stdin.write(mv)  # give that data to zstd comrpessor first
        digest.update(mv)  # hash in our process second

    gzjobret = gzipjob.wait()
    zsjobret = zstdjob.wait()

    end_time = time.time()
    print(
        f"Decompression, hashing and recompression took {end_time - start_time:.2f} seconds"
    )

    broken = False
    if gzjobret != 0:
        print(f"{myshlexjoin(gzipjob.args)} returned non-zero status: {gzjobret}")
        broken = True
    if zsjobret != 0:
        print(f"{myshlexjoin(zstdjob.args)} returned non-zero status: {zsjobret}")
        broken = True

    if broken:
        sys.exit(1)

    hash_start = time.time()
    okay = check_hashes(digest.hexdigest(), gzfname, tempfname)
    hash_end = time.time()
    print(f"Hashing two decompressed files took {hash_end - hash_start:.2f} seconds")

    if not okay:
        print("Some hashes mismatch - quitting.")
        sys.exit(2)

    print("All (pipe, original and new file) hashes match!")

    gzsize = os.path.getsize(gzfname)
    zssize = os.path.getsize(tempfname)

    print(f"Raw data size is:  {pretty_filesize(rawsize)}")
    print(f"Original size was: {pretty_filesize(gzsize)} in {gzfname}")
    print(f"Repacked size is:  {pretty_filesize(zssize)} in {finalname}")

    gzpercent = 100 * gzsize / rawsize
    zspercent = 100 * zssize / rawsize
    better = gzsize / zssize
    print(f"Ratio: {gzpercent:.3f}% -> {zspercent:.3f}% ({better:.2f}x better)")

    if gzsize <= zssize:
        # TODO: should this maybe be done only if --rm flag was passed?
        print(f"New file is bigger, deleting temporary file {tempfname}")
        os.remove(tempfname)
    else:
        print(f"renaming temporary file {tempfname} to {finalname}")
        os.rename(tempfname, finalname)
        if rm:
            print(f"removing original {gzfname}")
            os.remove(gzfname)

    print("All done.")


if __name__ == "__main__":
    main()
