# MC-Trimmer
Allows for fast, UI-less trimming of minecraft worlds.

## Usage
### Trim
```sh
mctrimmer trim [-h] -i INPUT_DIR [-p [THREADS]] -c {inhabited_time<15s, ...} [-b [BACKUP_DIR]] [-o [OUTPUT_DIR]]

Delete/Export select regions

options:
  -h, --help            show this help message and exit
  -i INPUT_DIR, --input-region INPUT_DIR
                        Directory to source the dimension files from. If no output directory is specified, in-place editing will be performed.
  -p [THREADS], --parallel [THREADS]
                        Parallelize the task. If no thread count is specified, the number of cpu cores -1 is taken instead.
  -c {inhabited_time<15s,inhabited_time<30s,inhabited_time<1m,inhabited_time<2m,inhabited_time<3m,inhabited_time<5m,inhabited_time<10m}, --criteria {inhabited_time<15s,inhabited_time<30s,inhabited_time<1m,inhabited_time<2m,inhabited_time<3m,inhabited_time<5m,inhabited_time<10m}
                        Pre-defined criteria by which to determine if a chunk should be trimmed or not.
  -b [BACKUP_DIR], --backup [BACKUP_DIR]
                        Backup regions affected by trimming to this directory. Defaults to './backup'
  -o [OUTPUT_DIR], --output-region [OUTPUT_DIR]
                        Directory to store the dimension files to. If unspecified, in-place editing will be performed by taking the input directory instead.
```


## Benchmark
Conditions:
```md
OS:                         Windows10 64bit
CPU:                        AMD 3700x
SSD:                        Corsair MP510 2TB
Total file size processed:  1.05 GB
Total output size:          436 MB
Total files:                120 region files, 120 entities files
```

Command being run:
```bat
Measure-Command {mctrimmer trim -i "./test_in" -o "%appdata%/.minecraft/saves/test" -b "./tests/test_backup" -c "inhabited_time<30s" -p}
```

Results:
```bat
TotalSeconds      : 3.3509565
TotalSeconds      : 4.4760565
TotalSeconds      : 3.3780054
TotalSeconds      : 3.4098966
TotalSeconds      : 4.0146584
TotalSeconds      : 3.7998296
TotalSeconds      : 3.8248743
TotalSeconds      : 3.9705653
```
