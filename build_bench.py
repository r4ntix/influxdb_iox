#!/usr/bin/env python3
import os
from datetime import datetime
from io import StringIO
import signal
from subprocess import run
from sys import argv
import time

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# modify this file to trigger a "rebuild",
# meaning cargo build is working with one change after the last cargo build
rebuild_trigger_file = 'read_buffer/src/column.rs'

cargo_toml_file = 'Cargo.toml'

quick_release_profile = '''
[profile.quick-release-tmp]
inherits = "release"
codegen-units = 16
lto = false
incremental = true
'''

influxdb_url = 'https://influxdb.aws.influxdata.io'
influxdb_org = 'InfluxData'
influxdb_bucket = 'iox-build-bench'


def benchmark_build(commit_hash):
    p = run('git checkout %s' % commit_hash, shell=True, text=True, capture_output=True)
    p.check_returncode()

    # ensure there's nothing to download and everything to build
    p = run('cargo fetch && cargo clean', shell=True, text=True, capture_output=True)
    p.check_returncode()

    print('benchmark debug build')
    debug_build_duration = time.time()
    p = run('cargo build', shell=True, text=True, capture_output=True)
    p.check_returncode()
    debug_build_duration = time.time() - debug_build_duration

    print('benchmark release build')
    release_build_duration = time.time()
    p = run('cargo build --release', shell=True, text=True, capture_output=True)
    p.check_returncode()
    release_build_duration = time.time() - release_build_duration

    print('tweak one file to trigger "rebuild"')
    replace_once(rebuild_trigger_file, 'unreachable!', 'panic!')

    print('benchmark debug rebuild')
    debug_rebuild_duration = time.time()
    p = run('cargo build', shell=True, text=True, capture_output=True)
    p.check_returncode()
    debug_rebuild_duration = time.time() - debug_rebuild_duration

    print('benchmark release rebuild')
    release_rebuild_duration = time.time()
    p = run('cargo build --release', shell=True, text=True, capture_output=True)
    p.check_returncode()
    release_rebuild_duration = time.time() - release_rebuild_duration

    print('reset')
    p = run('git restore .', shell=True, text=True, capture_output=True)
    p.check_returncode()
    p = run('cargo clean', shell=True, text=True, capture_output=True)
    p.check_returncode()

    print('benchmark quick release build')
    append(cargo_toml_file, quick_release_profile)
    quick_build_duration = time.time()
    p = run('cargo build --profile quick-release-tmp', shell=True, text=True, capture_output=True)
    p.check_returncode()
    quick_build_duration = time.time() - quick_build_duration

    print('tweak one file to trigger "rebuild"')
    replace_once(rebuild_trigger_file, 'unreachable!', 'panic!')

    print('benchmark quick release rebuild')
    quick_rebuild_duration = time.time()
    p = run('cargo build --profile quick-release-tmp', shell=True, text=True, capture_output=True)
    p.check_returncode()
    quick_rebuild_duration = time.time() - quick_rebuild_duration

    print('reset')
    p = run('git restore .', shell=True, text=True, capture_output=True)
    p.check_returncode()

    return (debug_build_duration, debug_rebuild_duration, release_build_duration,
            release_rebuild_duration, quick_build_duration, quick_rebuild_duration)


def replace_once(filename, to_find, replace_with):
    new_contents = StringIO()
    with open(filename, 'r') as f:
        for line in f:
            if to_find in line:
                new_contents.write(line.replace(to_find, replace_with))
                break
            new_contents.write(line)
        for line in f:
            new_contents.write(line)
    with open(filename, 'wt') as f:
        f.write(new_contents.getvalue())


def append(filename, new_text):
    with open(filename, 'a') as f:
        f.write(new_text)


def commits():
    for i in range(1000000):
        p = run(('git', 'show', 'HEAD~%d' % i, '-s', '--format=%ct %H'), text=True, capture_output=True)
        p.check_returncode()
        commit_time = int(p.stdout.split()[0])
        commit_hash = p.stdout.split()[1]
        yield commit_time, commit_hash


def signal_handler(sig, frame):
    exit(1)


signal.signal(signal.SIGINT, signal_handler)


def daily_to_present(start_date_string, output_filename):
    start_time = int(datetime.strptime(start_date_string, '%Y-%m-%d').timestamp())
    print('collecting weekly benchmarks between %s and today'
          % datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d'))
    print('writing to %s' % output_filename)
    f = open(output_filename, mode='w')
    f.write('date,hash,debug build,debug rebuild,release build,release rebuild,qr build,qr rebuild\n')
    f.flush()

    commit_generator = commits()

    to_benchmark = []

    interval_in_seconds = 1 * 24 * 60 * 60

    last_time, last_hash = next(commit_generator)
    threshold_time = last_time - (last_time % interval_in_seconds)

    while last_time > start_time:
        next_time, next_hash = next(commit_generator)
        if next_time < threshold_time:
            to_benchmark.append((last_time, last_hash))
            threshold_time -= interval_in_seconds
            while datetime.utcfromtimestamp(threshold_time).weekday() > 4:
                threshold_time -= interval_in_seconds
        last_time = next_time
        last_hash = next_hash

    for commit_time, commit_hash in reversed(to_benchmark):
        print('benchmarking %s %s' % (datetime.utcfromtimestamp(commit_time).strftime('%Y-%m-%d'), commit_hash))
        commit_date = datetime.utcfromtimestamp(commit_time).strftime('%Y-%m-%d')
        try:
            print('benchmark build %s' % commit_hash)
            (debug_build_duration, debug_rebuild_duration, release_build_duration,
             release_rebuild_duration, quick_build_duration, quick_rebuild_duration
             ) = benchmark_build(commit_hash)
            f.write(
                '%s,%s,%d,%d,%d,%d,%d,%d\n' % (
                    commit_date, commit_hash, debug_build_duration, debug_rebuild_duration,
                    release_build_duration, release_rebuild_duration, quick_build_duration,
                    quick_rebuild_duration))
        except Exception:
            print('failed to build %s %s' % (commit_date, commit_hash))


def single_commit(commit_hash, timestamp):
    token = os.environ.get('INFLUX_TOKEN')
    if not token:
        print('env var INFLUX_TOKEN is missing')
        exit(1)

    p = run(('git', 'show', commit_hash, '-s', '--format=%H'), text=True, capture_output=True)
    p.check_returncode()
    commit_hash = p.stdout
    print('benchmarking at commit %s' % commit_hash)

    (debug_build_duration, debug_rebuild_duration, release_build_duration,
     release_rebuild_duration, quick_build_duration, quick_rebuild_duration
     ) = benchmark_build(commit_hash)

    with InfluxDBClient(url=influxdb_url, token=token, org=influxdb_org) as client:
        if not client.ping():
            print('failed to connect to InfluxDB')
            exit(1)

        points = []
        points.append(Point('build').tag('profile', 'debug').tag('build_type', 'build')
                      .field('commit_hash', commit_hash)
                      .field('duration', debug_build_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        points.append(Point('build').tag('profile', 'debug').tag('build_type', 'rebuild')
                      .field('commit_hash', commit_hash)
                      .field('duration', debug_rebuild_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        points.append(Point('build').tag('profile', 'release').tag('build_type', 'build')
                      .field('commit_hash', commit_hash)
                      .field('duration', release_build_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        points.append(Point('build').tag('profile', 'release').tag('build_type', 'rebuild')
                      .field('commit_hash', commit_hash)
                      .field('duration', release_rebuild_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        points.append(Point('build').tag('profile', 'quick-release').tag('build_type', 'build')
                      .field('commit_hash', commit_hash)
                      .field('duration', quick_build_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        points.append(Point('build').tag('profile', 'quick-release').tag('build_type', 'rebuild')
                      .field('commit_hash', commit_hash)
                      .field('duration', quick_rebuild_duration)
                      .time(datetime.utcfromtimestamp(timestamp)))
        client.write_api(write_options=SYNCHRONOUS).write(bucket=influxdb_bucket, record=points)


if __name__ == '__main__':
    if argv[1] == 'daily_to_present':
        daily_to_present(argv[2], argv[3])
    elif argv[1] == 'single_commit':
        single_commit(argv[2], time.time())
    else:
        print('%s not implemented' % argv[1])
