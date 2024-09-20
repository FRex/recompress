import subprocess
import hashlib
import sys

gzfname = sys.argv[1]


args = ["zstd", "-d", "--stdout", gzfname]
gzipjob = subprocess.Popen(args, stdout=subprocess.PIPE)


zsfname = gzfname + ".zst"

args = ["zstd", "-o", zsfname]
zstdjob = subprocess.Popen(args, stdin=subprocess.PIPE)

digest = hashlib.sha256()
while True:
    data = gzipjob.stdout.read(32 * 1024)
    if not data:
        break

    digest.update(data)
    zstdjob.stdin.write(data)

    # print(len(data))

zstdjob.stdin.close()  # need to close stdin to make zstd process end

print(gzipjob.wait())
print(zstdjob.wait())


def getdigest(fname: str) -> str:
    args = ["zstd", "-d", "--stdout", fname]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as job:
        return hashlib.file_digest(job.stdout, hashlib.sha256).hexdigest()


print(digest.hexdigest())
print(getdigest(gzfname))
print(getdigest(zsfname))
