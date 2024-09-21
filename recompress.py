import subprocess
import humanize
import hashlib
import sys
import os


def pretty_filesize(fsize: int) -> str:
    return humanize.naturalsize(fsize, True)


def getdigest(fname: str) -> str:
    args = ["zstd", "-d", "--stdout", fname]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as job:
        return hashlib.file_digest(job.stdout, hashlib.sha256).hexdigest()


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


gzfname = sys.argv[1]


args = ["zstd", "-d", "--stdout", gzfname]
gzipjob = subprocess.Popen(args, stdout=subprocess.PIPE)


zsfname = gzfname + ".zst"

args = ["zstd", "--quiet", "-o", zsfname]
zstdjob = subprocess.Popen(args, stdin=subprocess.PIPE)

digest = hashlib.sha256()

rawsize = 0
while True:
    data = gzipjob.stdout.read(64 * 1024)
    if not data:
        zstdjob.stdin.close()  # need to close stdin to make zstd process end
        break

    rawsize += len(data)
    digest.update(data)
    zstdjob.stdin.write(data)


gzjobret = gzipjob.wait()
zsjobret = zstdjob.wait()


okay = check_hashes(digest.hexdigest(), gzfname, zsfname)
if okay:
    print("All hashes match!")

gzsize = os.path.getsize(gzfname)
zssize = os.path.getsize(zsfname)

print(f"Raw data size is:  {pretty_filesize(rawsize)}")
print(f"Original size was: {pretty_filesize(gzsize)} in {gzfname}")
print(f"Repacked size is:  {pretty_filesize(zssize)} in {zsfname}")
