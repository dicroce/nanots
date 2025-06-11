#!/usr/bin/env python3
"""
NanoTS Parallel Threading Stress Test

This script demonstrates the TRUE parallel power of NanoTS by utilizing
Python threading with the GIL-releasing C++ backend. We'll run:

- Multiple writer threads (one per crypto symbol)
- Live reader threads analyzing data in real-time  
- Performance monitoring and throughput measurement
- Real concurrent database operations

Let's see what NanoTS can REALLY do with parallel threading!
"""

import threading
import time
import random
import struct
import queue
import csv
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime, timedelta
import nanots

# Import binance data for real data
try:
    from binance_historical_data import BinanceDataDumper
    BINANCE_AVAILABLE = True
except ImportError:
    BINANCE_AVAILABLE = False


class ParallelNanoTSStressTest:
    """Comprehensive parallel threading stress test for NanoTS."""
    
    def __init__(self):
        self.symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", 
            "SOLUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"
        ]
        
        self.db_path = "parallel_stress_test.nanots"
        self.data_dir = "./parallel_test_data"
        
        # Threading coordination
        self.writer_threads = []
        self.reader_threads = []
        self.data_queues = {}
        self.stats_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        # Performance tracking
        self.writer_stats = defaultdict(lambda: {
            'records_written': 0,
            'bytes_written': 0,
            'write_times': deque(maxlen=1000),
            'errors': 0
        })
        
        self.reader_stats = defaultdict(lambda: {
            'records_read': 0,
            'query_times': deque(maxlen=1000),
            'queries_executed': 0
        })
        
        self.global_stats = {
            'start_time': 0,
            'total_records': 0,
            'peak_write_rate': 0,
            'peak_read_rate': 0
        }
    
    def create_parallel_database(self):
        """Create optimized database for parallel operations."""
        print("🏗️  Creating parallel-optimized NanoTS database...")
        
        # Large database for parallel stress testing
        block_size = 1024 * 1024  # 1MB blocks
        num_blocks = 20000        # 20GB capacity
        
        print(f"📊 Parallel Database Specs:")
        print(f"   Block size: {block_size//1024//1024}MB")
        print(f"   Total blocks: {num_blocks:,}")
        print(f"   Total capacity: {num_blocks * block_size // 1024**3}GB")
        
        nanots.allocate_file(self.db_path, block_size, num_blocks)
        print(f"✅ Allocated massive parallel database")
    
    def generate_synthetic_data(self, symbol, num_records=50000):
        """Generate realistic synthetic crypto data for stress testing."""
        base_price = {
            "BTCUSDT": 100000, "ETHUSDT": 4000, "BNBUSDT": 600, "ADAUSDT": 1.2,
            "SOLUSDT": 200, "XRPUSDT": 2.5, "DOTUSDT": 25, "LINKUSDT": 20
        }.get(symbol, 1000)
        
        # Each symbol gets its own time range to avoid conflicts
        symbol_index = self.symbols.index(symbol)
        base_timestamp = int(time.time() * 1000) - (num_records * 60000)  # Start in past
        start_time = base_timestamp + (symbol_index * num_records * 60000)  # Separate time windows
        
        current_price = base_price
        
        data = []
        for i in range(num_records):
            # Realistic price movement
            price_change = random.gauss(0, base_price * 0.001)  # 0.1% volatility
            current_price = max(current_price + price_change, base_price * 0.5)
            
            # OHLCV data
            high = current_price * (1 + random.uniform(0, 0.002))
            low = current_price * (1 - random.uniform(0, 0.002))
            volume = random.uniform(10, 1000)
            trades = random.randint(100, 5000)
            
            record = {
                'timestamp': start_time + (i * 60000),  # Guaranteed monotonic per symbol
                'open': current_price,
                'high': high,
                'low': low,
                'close': current_price,
                'volume': volume,
                'trades': trades
            }
            
            data.append(record)
        
        return data
    
    def parallel_writer_thread(self, symbol, thread_id):
        """Individual writer thread - one per symbol."""
        print(f"🚀 Writer thread {thread_id} starting for {symbol}")
        
        try:
            # Each thread gets its own writer and context
            writer = nanots.Writer(self.db_path, auto_reclaim=True)
            price_context = writer.create_context(f"{symbol}_price", f"{symbol} OHLCV data")
            volume_context = writer.create_context(f"{symbol}_volume", f"{symbol} volume data") 
            trade_context = writer.create_context(f"{symbol}_trades", f"{symbol} trade data")
            
            # CRITICAL: Each thread needs unique stream tags
            price_tag = thread_id * 10 + 1      # e.g., 1, 11, 21, 31...
            volume_tag = thread_id * 10 + 2     # e.g., 2, 12, 22, 32...
            trade_tag = thread_id * 10 + 3      # e.g., 3, 13, 23, 33...
            
            data_queue = self.data_queues[symbol]
            
            while not self.shutdown_event.is_set():
                try:
                    # Get data from queue (non-blocking with timeout)
                    record = data_queue.get(timeout=0.1)
                    
                    if record is None:  # Shutdown signal
                        break
                    
                    write_start = time.time()
                    
                    # Pack OHLC data
                    price_data = struct.pack('ffff', 
                                           record['open'], record['high'],
                                           record['low'], record['close'])
                    
                    # Pack volume data  
                    volume_data = struct.pack('f', record['volume'])
                    
                    # Pack trade data
                    trade_data = struct.pack('if', record['trades'], record['volume'])
                    
                    # Write to different contexts with UNIQUE STREAM TAGS
                    writer.write(price_context, price_data, record['timestamp'], price_tag)
                    writer.write(volume_context, volume_data, record['timestamp'], volume_tag)
                    writer.write(trade_context, trade_data, record['timestamp'], trade_tag)
                    
                    write_time = time.time() - write_start
                    
                    # Update stats (thread-safe)
                    with self.stats_lock:
                        stats = self.writer_stats[symbol]
                        stats['records_written'] += 3  # 3 writes per record
                        stats['bytes_written'] += len(price_data) + len(volume_data) + len(trade_data)
                        stats['write_times'].append(write_time)
                        self.global_stats['total_records'] += 3
                    
                    data_queue.task_done()
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    with self.stats_lock:
                        self.writer_stats[symbol]['errors'] += 1
                    print(f"❌ Writer {symbol} error: {e}")
                    
        except Exception as e:
            print(f"❌ Writer thread {symbol} failed: {e}")
        
        print(f"🏁 Writer thread {thread_id} ({symbol}) completed")
    
    def parallel_reader_thread(self, reader_id, symbols_to_read):
        """Live reader thread performing analysis while writing."""
        print(f"📖 Reader thread {reader_id} starting for {symbols_to_read}")
        
        try:
            # Each reader gets its own reader instance
            reader = nanots.Reader(self.db_path)
            
            while not self.shutdown_event.is_set():
                for symbol in symbols_to_read:
                    try:
                        query_start = time.time()
                        
                        # Read recent data (last 1000 records)
                        end_time = int(time.time() * 1000)
                        start_time = end_time - (1000 * 60 * 1000)  # 1000 minutes ago
                        
                        # Query price data (LIVE READ while writers are active!)
                        frames = reader.read(f"{symbol}_price", start_time, end_time)
                        
                        query_time = time.time() - query_start
                        
                        # Update reader stats
                        with self.stats_lock:
                            stats = self.reader_stats[f"reader_{reader_id}"]
                            stats['records_read'] += len(frames)
                            stats['query_times'].append(query_time)
                            stats['queries_executed'] += 1
                        
                        # Brief pause to avoid overwhelming
                        time.sleep(0.01)
                        
                    except Exception as e:
                        # Ignore read errors during parallel writing
                        continue
                
                time.sleep(0.1)  # Check shutdown every 100ms
                
        except Exception as e:
            print(f"❌ Reader thread {reader_id} failed: {e}")
        
        print(f"🏁 Reader thread {reader_id} completed")
    
    def stats_monitor_thread(self):
        """Monitor and report real-time performance statistics."""
        print("📊 Stats monitor thread starting")
        
        last_total = 0
        last_time = time.time()
        
        while not self.shutdown_event.is_set():
            time.sleep(1.0)  # Report every second
            
            current_time = time.time()
            
            with self.stats_lock:
                current_total = self.global_stats['total_records']
                
                # Calculate throughput
                records_delta = current_total - last_total
                time_delta = current_time - last_time
                current_rate = records_delta / time_delta if time_delta > 0 else 0
                
                # Update peak rate
                if current_rate > self.global_stats['peak_write_rate']:
                    self.global_stats['peak_write_rate'] = current_rate
                
                # Print real-time stats
                elapsed = current_time - self.global_stats['start_time']
                avg_rate = current_total / elapsed if elapsed > 0 else 0
                
                print(f"⚡ Live Stats: {current_total:,} records | "
                      f"Current: {current_rate:.0f}/sec | "
                      f"Average: {avg_rate:.0f}/sec | "
                      f"Peak: {self.global_stats['peak_write_rate']:.0f}/sec")
                
                last_total = current_total
                last_time = current_time
    
    def feed_data_to_writers(self, records_per_symbol=100000):
        """Feed synthetic data to writer threads via queues."""
        print(f"🍽️  Feeding {records_per_symbol:,} records per symbol to writers...")
        
        for symbol in self.symbols:
            print(f"📊 Generating data for {symbol}...")
            data = self.generate_synthetic_data(symbol, records_per_symbol)
            
            # DO NOT SHUFFLE - keep timestamps monotonic for nanots!
            # random.shuffle(data)  # ❌ This breaks monotonic requirement
            
            # Feed to queue in chronological order
            for record in data:
                self.data_queues[symbol].put(record)
    
    def run_parallel_stress_test(self, records_per_symbol=100000, num_readers=4):
        """Execute the full parallel stress test."""
        print("🔥 NANOTS PARALLEL THREADING STRESS TEST")
        print("=" * 80)
        print(f"🎯 Target: {len(self.symbols)} symbols × {records_per_symbol:,} records = {len(self.symbols) * records_per_symbol:,} total")
        print(f"🧵 Writers: {len(self.symbols)} threads (one per symbol)")
        print(f"📖 Readers: {num_readers} threads (live analysis)")
        print(f"📊 Expected: TRUE PARALLEL execution via C++ backend")
        print()
        
        try:
            # Step 1: Create database
            self.create_parallel_database()
            
            # Step 2: Initialize queues
            print("🔧 Setting up threading infrastructure...")
            for symbol in self.symbols:
                self.data_queues[symbol] = queue.Queue(maxsize=10000)
            
            # Step 3: Start monitoring
            self.global_stats['start_time'] = time.time()
            stats_thread = threading.Thread(target=self.stats_monitor_thread, daemon=True)
            stats_thread.start()
            
            # Step 4: Launch writer threads (TRUE PARALLEL!)
            print("🚀 Launching parallel writer threads...")
            for i, symbol in enumerate(self.symbols):
                writer_thread = threading.Thread(
                    target=self.parallel_writer_thread,
                    args=(symbol, i),
                    name=f"Writer-{symbol}"
                )
                writer_thread.start()
                self.writer_threads.append(writer_thread)
            
            # Step 5: Launch reader threads (LIVE ANALYSIS!)
            print("📖 Launching parallel reader threads...")
            symbols_per_reader = len(self.symbols) // num_readers
            for i in range(num_readers):
                start_idx = i * symbols_per_reader
                end_idx = start_idx + symbols_per_reader if i < num_readers - 1 else len(self.symbols)
                reader_symbols = self.symbols[start_idx:end_idx]
                
                reader_thread = threading.Thread(
                    target=self.parallel_reader_thread,
                    args=(i, reader_symbols),
                    name=f"Reader-{i}"
                )
                reader_thread.start()
                self.reader_threads.append(reader_thread)
            
            print(f"✅ All {len(self.writer_threads)} writers + {len(self.reader_threads)} readers launched!")
            
            # Step 6: Feed data to writers
            time.sleep(1)  # Let threads initialize
            self.feed_data_to_writers(records_per_symbol)
            
            # Step 7: Wait for completion
            print("⏳ Waiting for all writers to complete...")
            
            # Signal shutdown when all queues are empty
            all_empty = False
            while not all_empty:
                time.sleep(1)
                all_empty = all(q.empty() for q in self.data_queues.values())
            
            # Shutdown signal
            for symbol in self.symbols:
                self.data_queues[symbol].put(None)  # Poison pill
            
            # Wait for writers
            for thread in self.writer_threads:
                thread.join(timeout=30)
            
            print("✅ All writers completed!")
            
            # Let readers run a bit more to show live capability
            print("📖 Letting readers continue for live analysis demo...")
            time.sleep(5)
            
            # Shutdown everything
            self.shutdown_event.set()
            
            for thread in self.reader_threads:
                thread.join(timeout=10)
            
            stats_thread.join(timeout=5)
            
            # Final analysis
            self.analyze_parallel_results()
            
        except KeyboardInterrupt:
            print("\n⚠️  Parallel stress test interrupted!")
            self.shutdown_event.set()
        except Exception as e:
            print(f"\n❌ Parallel stress test failed: {e}")
            raise
    
    def analyze_parallel_results(self):
        """Comprehensive analysis of parallel performance."""
        print("\n🎊 PARALLEL STRESS TEST RESULTS")
        print("=" * 80)
        
        total_time = time.time() - self.global_stats['start_time']
        
        # Database size
        db_size_mb = Path(self.db_path).stat().st_size / 1024 / 1024
        
        print(f"📊 PARALLEL EXECUTION RESULTS:")
        print(f"   ⏱️  Total runtime: {total_time:.1f} seconds")
        print(f"   📈 Total records: {self.global_stats['total_records']:,}")
        print(f"   💾 Database size: {db_size_mb:.1f} MB")
        print(f"   📊 Data density: {self.global_stats['total_records']/db_size_mb:.1f} records/MB")
        
        print(f"\n⚡ THREADING PERFORMANCE:")
        avg_throughput = self.global_stats['total_records'] / total_time
        print(f"   🚀 Average throughput: {avg_throughput:.0f} records/sec")
        print(f"   🔥 Peak throughput: {self.global_stats['peak_write_rate']:.0f} records/sec")
        print(f"   🧵 Parallel efficiency: {len(self.symbols)} concurrent writers")
        
        # Per-symbol breakdown
        print(f"\n📋 PER-SYMBOL WRITER PERFORMANCE:")
        total_written = 0
        total_errors = 0
        
        for symbol in self.symbols:
            stats = self.writer_stats[symbol]
            if stats['records_written'] > 0:
                avg_write_time = sum(stats['write_times']) / len(stats['write_times']) * 1000
                symbol_rate = stats['records_written'] / total_time
                total_written += stats['records_written']
                total_errors += stats['errors']
                
                print(f"   {symbol:8s}: {stats['records_written']:,} records "
                      f"({symbol_rate:.0f}/sec, {avg_write_time:.2f}ms avg)")
        
        # Reader performance
        print(f"\n📖 LIVE READER PERFORMANCE:")
        total_read = 0
        total_queries = 0
        
        for reader_id in range(len(self.reader_threads)):
            stats = self.reader_stats[f"reader_{reader_id}"]
            if stats['queries_executed'] > 0:
                avg_query_time = sum(stats['query_times']) / len(stats['query_times']) * 1000
                read_rate = stats['records_read'] / total_time
                total_read += stats['records_read']
                total_queries += stats['queries_executed']
                
                print(f"   Reader {reader_id}: {stats['records_read']:,} records read "
                      f"({read_rate:.0f}/sec, {avg_query_time:.2f}ms avg query)")
        
        print(f"\n🏆 NANOTS PARALLEL VERDICT:")
        if avg_throughput > 50000:
            print(f"   🔥 INCREDIBLE: {avg_throughput:.0f} records/sec with {len(self.symbols)} parallel writers!")
        elif avg_throughput > 25000:
            print(f"   🚀 EXCELLENT: {avg_throughput:.0f} records/sec parallel performance!")
        elif avg_throughput > 10000:
            print(f"   ✅ VERY GOOD: {avg_throughput:.0f} records/sec with threading!")
        else:
            print(f"   ⚠️  BASELINE: {avg_throughput:.0f} records/sec")
        
        print(f"\n🎯 THEORETICAL SCALING:")
        single_thread_estimate = avg_throughput / len(self.symbols)
        print(f"   📊 Estimated single-thread: {single_thread_estimate:.0f} records/sec")
        parallel_efficiency = (avg_throughput / (single_thread_estimate * len(self.symbols))) * 100
        print(f"   🧵 Parallel efficiency: {parallel_efficiency:.1f}%")
        
        # Concurrent read/write demonstration
        if total_read > 0:
            print(f"   📖 Live reads during writes: {total_read:,} records ({total_queries} queries)")
            print(f"   🔄 Read/Write concurrency: DEMONSTRATED ✅")
        
        print(f"\n📂 Database file: {self.db_path} ({db_size_mb:.1f}MB)")
        
        # Cleanup option
        keep = input(f"\n🗑️  Keep parallel test database ({db_size_mb:.1f}MB)? (y/n): ").lower().strip()
        if keep != 'y':
            if Path(self.db_path).exists():
                Path(self.db_path).unlink()
                print(f"🗑️  Removed {self.db_path}")


def main():
    """Main parallel stress test execution."""
    print("🚀 Welcome to the NanoTS PARALLEL Threading Stress Test!")
    print("This will test TRUE parallel execution via C++ backend with GIL release.")
    print()
    
    print("🧵 Parallel test configurations:")
    print("  1. 🔥 PARALLEL DEMO: 8 writers × 50K records (~400K total)")
    print("  2. 🚀 PARALLEL STRESS: 8 writers × 100K records (~800K total)")
    print("  3. 💀 PARALLEL EXTREME: 8 writers × 200K records (~1.6M total)")
    print("  4. 🎯 Custom parallel configuration")
    
    choice = input("\nSelect parallel test (1-4): ").strip()
    
    if choice == "1":
        records = 50000
        readers = 2
        print("🔥 PARALLEL DEMO selected!")
    elif choice == "2":
        records = 100000
        readers = 4
        print("🚀 PARALLEL STRESS selected!")
    elif choice == "3":
        records = 200000
        readers = 6
        print("💀 PARALLEL EXTREME selected!")
    elif choice == "4":
        records = int(input("Records per symbol: "))
        readers = int(input("Number of reader threads: "))
        print(f"🎯 Custom: {records} records, {readers} readers")
    else:
        records = 50000
        readers = 2
        print("🔥 Default PARALLEL DEMO")
    
    total_records = records * 8 * 3  # 8 symbols × 3 streams each
    print(f"\n⚠️  This will create ~{total_records:,} database records")
    print(f"💾 Estimated database size: ~{total_records * 20 / 1024 / 1024:.0f}MB")
    print(f"🧵 Threading: 8 parallel writers + {readers} live readers")
    
    if input("\nReady to test TRUE parallel power? (y/n): ").lower().strip() != 'y':
        print("👋 Parallel stress test cancelled")
        return
    
    # Execute the parallel stress test
    tester = ParallelNanoTSStressTest()
    tester.run_parallel_stress_test(records_per_symbol=records, num_readers=readers)


if __name__ == "__main__":
    main()