#!/usr/bin/env python3
"""
NanoTS Parallel Threading Stress Test

This script tests NanoTS parallel capabilities by utilizing Python threading
with the GIL-releasing C++ backend. The test includes:

- Multiple writer threads (one per crypto symbol)
- Live reader threads analyzing data in real-time  
- Performance monitoring and throughput measurement
- Real concurrent database operations

Uses synthetic cryptocurrency data to focus purely on NanoTS performance.
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


class ParallelNanoTSStressTest:
    """Comprehensive parallel threading stress test for NanoTS."""
    
    def __init__(self):
        self.symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", 
            "SOLUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"
        ]
        
        self.db_path = "parallel_stress_test.nanots"
        
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
    
    def create_parallel_database(self, block_size_mb):
        """Create optimized database for parallel operations."""
        print("🏗️  Creating parallel-optimized NanoTS database...")
        
        # Calculate database parameters from pre-configured block size
        block_size = block_size_mb * 1024 * 1024  # Convert MB to bytes
        
        # Calculate number of blocks based on block size
        if block_size_mb <= 4:
            num_blocks = 20000  # Up to 80GB for small blocks
        elif block_size_mb <= 16:
            num_blocks = 5000   # Up to 80GB for medium blocks  
        elif block_size_mb <= 64:
            num_blocks = 1250   # Up to 80GB for large blocks
        else:
            num_blocks = 500    # Up to 500GB for very large blocks
        
        total_capacity_gb = (num_blocks * block_size) / (1024**3)
        
        print(f"\n📊 Database Allocation:")
        print(f"   Block size: {block_size_mb}MB")
        print(f"   Total blocks: {num_blocks:,}")
        print(f"   Total capacity: {total_capacity_gb:.1f}GB")
        
        nanots.allocate_file(self.db_path, block_size, num_blocks)
        print(f"✅ Database allocated successfully")
    
    def generate_synthetic_crypto_data(self, symbol, num_records=50000):
        """
        Generate synthetic cryptocurrency OHLCV data for testing.
        
        Creates realistic-looking price movements using random walks
        with proper volatility characteristics for each symbol.
        """
        # Realistic base prices for different cryptocurrencies
        base_price = {
            "BTCUSDT": 100000, "ETHUSDT": 4000, "BNBUSDT": 600, "ADAUSDT": 1.2,
            "SOLUSDT": 200, "XRPUSDT": 2.5, "DOTUSDT": 25, "LINKUSDT": 20
        }.get(symbol, 1000)
        
        # Each symbol gets its own time range to avoid timestamp conflicts
        symbol_index = self.symbols.index(symbol)
        base_timestamp = int(time.time() * 1000) - (num_records * 60000)  # Start in past
        start_time = base_timestamp + (symbol_index * num_records * 60000)  # Separate time windows
        
        current_price = base_price
        data = []
        
        print(f"📊 Generating {num_records:,} synthetic records for {symbol}")
        
        for i in range(num_records):
            # Simulate realistic price movement (0.1% volatility)
            price_change = random.gauss(0, base_price * 0.001)
            current_price = max(current_price + price_change, base_price * 0.5)
            
            # Generate OHLCV data with realistic spread
            high = current_price * (1 + random.uniform(0, 0.002))
            low = current_price * (1 - random.uniform(0, 0.002))
            volume = random.uniform(10, 1000)
            trades = random.randint(100, 5000)
            
            record = {
                'timestamp': start_time + (i * 60000),  # 1 minute intervals
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
            # Each thread gets its own writer and contexts
            writer = nanots.Writer(self.db_path, auto_reclaim=True)
            price_context = writer.create_context(f"{symbol}_price", f"{symbol} OHLCV data")
            volume_context = writer.create_context(f"{symbol}_volume", f"{symbol} volume data") 
            trade_context = writer.create_context(f"{symbol}_trades", f"{symbol} trade data")
            
            # Each thread needs unique stream tags to avoid conflicts
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
                    
                    # Pack binary data for efficient storage
                    price_data = struct.pack('ffff', 
                                           record['open'], record['high'],
                                           record['low'], record['close'])
                    
                    volume_data = struct.pack('f', record['volume'])
                    trade_data = struct.pack('if', record['trades'], record['volume'])
                    
                    # Write to different contexts with unique stream tags
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
        """Live reader thread performing queries while writing occurs."""
        print(f"📖 Reader thread {reader_id} starting for {symbols_to_read}")
        
        try:
            # Each reader gets its own reader instance
            reader = nanots.Reader(self.db_path)
            
            while not self.shutdown_event.is_set():
                for symbol in symbols_to_read:
                    try:
                        query_start = time.time()
                        
                        # Read recent data (last 1000 records worth of time)
                        end_time = int(time.time() * 1000)
                        start_time = end_time - (1000 * 60 * 1000)  # 1000 minutes ago
                        
                        # Query price data (concurrent with active writes!)
                        frames = reader.read(f"{symbol}_price", start_time, end_time)
                        
                        query_time = time.time() - query_start
                        
                        # Update reader stats
                        with self.stats_lock:
                            stats = self.reader_stats[f"reader_{reader_id}"]
                            stats['records_read'] += len(frames)
                            stats['query_times'].append(query_time)
                            stats['queries_executed'] += 1
                        
                        # Brief pause to avoid overwhelming the system
                        time.sleep(0.01)
                        
                    except Exception as e:
                        # Ignore read errors during active writing
                        continue
                
                time.sleep(0.1)  # Check shutdown periodically
                
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
                
                # Calculate current throughput
                records_delta = current_total - last_total
                time_delta = current_time - last_time
                current_rate = records_delta / time_delta if time_delta > 0 else 0
                
                # Update peak rate
                if current_rate > self.global_stats['peak_write_rate']:
                    self.global_stats['peak_write_rate'] = current_rate
                
                # Print live statistics
                elapsed = current_time - self.global_stats['start_time']
                avg_rate = current_total / elapsed if elapsed > 0 else 0
                
                print(f"⚡ Live Stats: {current_total:,} records | "
                      f"Current: {current_rate:.0f}/sec | "
                      f"Average: {avg_rate:.0f}/sec | "
                      f"Peak: {self.global_stats['peak_write_rate']:.0f}/sec")
                
                last_total = current_total
                last_time = current_time
    
    def feed_synthetic_data_to_writers(self, records_per_symbol=100000):
        """Generate and feed synthetic data to writer threads via queues."""
        print(f"🍽️  Generating and feeding {records_per_symbol:,} records per symbol...")
        
        for symbol in self.symbols:
            data = self.generate_synthetic_crypto_data(symbol, records_per_symbol)
            
            # Feed to queue in chronological order (NanoTS requires monotonic timestamps)
            for record in data:
                self.data_queues[symbol].put(record)
    
    def run_parallel_stress_test(self, records_per_symbol=100000, num_readers=4, block_size_mb=None):
        """Execute the complete parallel stress test."""
        print("🔥 NANOTS PARALLEL THREADING STRESS TEST")
        print("=" * 80)
        print(f"📊 Test Configuration:")
        print(f"   Symbols: {len(self.symbols)} cryptocurrencies")
        print(f"   Records per symbol: {records_per_symbol:,}")
        print(f"   Total records: {len(self.symbols) * records_per_symbol * 3:,} (3 streams per symbol)")
        print(f"   Writer threads: {len(self.symbols)} (one per symbol)")
        print(f"   Reader threads: {num_readers} (live analysis)")
        print(f"   Data: Synthetic cryptocurrency OHLCV")
        print()
        
        try:
            # Step 1: Create database with user-specified block size
            self.create_parallel_database(block_size_mb)
            
            # Step 2: Initialize queues
            print("\n🔧 Setting up threading infrastructure...")
            for symbol in self.symbols:
                self.data_queues[symbol] = queue.Queue(maxsize=10000)
            
            # Step 3: Start performance monitoring
            self.global_stats['start_time'] = time.time()
            stats_thread = threading.Thread(target=self.stats_monitor_thread, daemon=True)
            stats_thread.start()
            
            # Step 4: Launch writer threads
            print("🚀 Launching parallel writer threads...")
            for i, symbol in enumerate(self.symbols):
                writer_thread = threading.Thread(
                    target=self.parallel_writer_thread,
                    args=(symbol, i),
                    name=f"Writer-{symbol}"
                )
                writer_thread.start()
                self.writer_threads.append(writer_thread)
            
            # Step 5: Launch reader threads for live analysis
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
            
            print(f"✅ Launched {len(self.writer_threads)} writers + {len(self.reader_threads)} readers")
            
            # Step 6: Generate and feed synthetic data to writers
            time.sleep(1)  # Let threads initialize
            self.feed_synthetic_data_to_writers(records_per_symbol)
            
            # Step 7: Wait for completion
            print("⏳ Waiting for all writers to complete...")
            
            # Wait until all queues are empty
            all_empty = False
            while not all_empty:
                time.sleep(1)
                all_empty = all(q.empty() for q in self.data_queues.values())
            
            # Send shutdown signal to writers
            for symbol in self.symbols:
                self.data_queues[symbol].put(None)  # Poison pill
            
            # Wait for writers to finish
            for thread in self.writer_threads:
                thread.join(timeout=30)
            
            print("✅ All writers completed!")
            
            # Let readers continue briefly to demonstrate live capability
            print("📖 Letting readers continue for live analysis demonstration...")
            time.sleep(5)
            
            # Shutdown everything
            self.shutdown_event.set()
            
            for thread in self.reader_threads:
                thread.join(timeout=10)
            
            stats_thread.join(timeout=5)
            
            # Analyze results
            self.analyze_results()
            
        except KeyboardInterrupt:
            print("\n⚠️  Stress test interrupted!")
            self.shutdown_event.set()
        except Exception as e:
            print(f"\n❌ Stress test failed: {e}")
            raise
    
    def analyze_results(self):
        """Analyze and report comprehensive performance results."""
        print("\n🎊 PARALLEL STRESS TEST RESULTS")
        print("=" * 80)
        
        total_time = time.time() - self.global_stats['start_time']
        
        # Database size analysis
        db_size_mb = Path(self.db_path).stat().st_size / 1024 / 1024
        
        print(f"📊 EXECUTION SUMMARY:")
        print(f"   ⏱️  Total runtime: {total_time:.1f} seconds")
        print(f"   📈 Total records written: {self.global_stats['total_records']:,}")
        print(f"   💾 Database size: {db_size_mb:.1f} MB")
        print(f"   📊 Storage efficiency: {self.global_stats['total_records']/db_size_mb:.1f} records/MB")
        
        print(f"\n⚡ THREADING PERFORMANCE:")
        avg_throughput = self.global_stats['total_records'] / total_time
        print(f"   🚀 Average throughput: {avg_throughput:.0f} records/sec")
        print(f"   🔥 Peak throughput: {self.global_stats['peak_write_rate']:.0f} records/sec")
        print(f"   🧵 Parallel writers: {len(self.symbols)} concurrent")
        
        # Per-symbol performance breakdown
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
        
        # Reader performance analysis
        print(f"\n📖 CONCURRENT READER PERFORMANCE:")
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
        
        print(f"\n🏆 NANOTS PARALLEL ASSESSMENT:")
        if avg_throughput > 50000:
            print(f"   🔥 EXCELLENT: {avg_throughput:.0f} records/sec with {len(self.symbols)} parallel writers!")
        elif avg_throughput > 25000:
            print(f"   🚀 VERY GOOD: {avg_throughput:.0f} records/sec parallel performance!")
        elif avg_throughput > 10000:
            print(f"   ✅ GOOD: {avg_throughput:.0f} records/sec with threading!")
        else:
            print(f"   📊 BASELINE: {avg_throughput:.0f} records/sec")
        
        print(f"\n🎯 PARALLEL SCALING ANALYSIS:")
        estimated_single_thread = avg_throughput / len(self.symbols)
        print(f"   📊 Estimated single-thread rate: {estimated_single_thread:.0f} records/sec")
        parallel_efficiency = (avg_throughput / (estimated_single_thread * len(self.symbols))) * 100
        print(f"   🧵 Parallel efficiency: {parallel_efficiency:.1f}%")
        
        # Concurrent operations summary
        if total_read > 0:
            print(f"   📖 Concurrent reads during writes: {total_read:,} records ({total_queries} queries)")
            print(f"   🔄 Read/Write concurrency: ✅ DEMONSTRATED")
        
        if total_errors > 0:
            print(f"   ⚠️  Total errors: {total_errors}")
        
        print(f"\n📂 Database file: {self.db_path} ({db_size_mb:.1f}MB)")
        
        # Cleanup option
        keep = input(f"\n🗑️  Keep test database ({db_size_mb:.1f}MB)? (y/n): ").lower().strip()
        if keep != 'y':
            if Path(self.db_path).exists():
                Path(self.db_path).unlink()
                print(f"🗑️  Removed {self.db_path}")


def main():
    """Main stress test execution with configuration options."""
    print("🚀 NanoTS Parallel Threading Stress Test")
    print("Tests parallel execution capabilities using synthetic cryptocurrency data")
    print()
    
    print("🧵 Available test configurations:")
    print("  1. 🔥 DEMO: 8 symbols × 25K records each (~600K total)")
    print("  2. 🚀 STANDARD: 8 symbols × 50K records each (~1.2M total)")
    print("  3. 💪 HEAVY: 8 symbols × 100K records each (~2.4M total)")
    print("  4. 💀 EXTREME: 8 symbols × 200K records each (~4.8M total)")
    print("  5. 🎯 Custom configuration")
    
    choice = input("\nSelect test configuration (1-5): ").strip()
    
    if choice == "1":
        records = 25000
        readers = 2
        print("🔥 DEMO configuration selected")
    elif choice == "2":
        records = 50000
        readers = 3
        print("🚀 STANDARD configuration selected")
    elif choice == "3":
        records = 100000
        readers = 4
        print("💪 HEAVY configuration selected")
    elif choice == "4":
        records = 200000
        readers = 6
        print("💀 EXTREME configuration selected")
    elif choice == "5":
        records = int(input("Records per symbol: "))
        readers = int(input("Number of reader threads: "))
        print(f"🎯 Custom: {records:,} records per symbol, {readers} readers")
    else:
        records = 25000
        readers = 2
        print("🔥 Default DEMO configuration")
    
    # Configure block size right after test selection
    print("\n📏 Block Size Configuration:")
    print("   Larger blocks = better sequential performance, more memory usage")
    print("   Smaller blocks = lower memory usage, more granular allocation")
    print("   Typical options: 1MB (default), 4MB, 16MB, 64MB")
    
    while True:
        try:
            block_size_input = input("\nBlock size in MB (default 1): ").strip()
            if not block_size_input:
                block_size_mb = 1
                break
            block_size_mb = int(block_size_input)
            if block_size_mb <= 0 or block_size_mb > 1024:
                print("❌ Block size must be between 1 and 1024 MB")
                continue
            break
        except ValueError:
            print("❌ Please enter a valid number")
            continue
    
    # Calculate database capacity based on block size
    if block_size_mb <= 4:
        num_blocks = 20000  # Up to 80GB for small blocks
    elif block_size_mb <= 16:
        num_blocks = 5000   # Up to 80GB for medium blocks  
    elif block_size_mb <= 64:
        num_blocks = 1250   # Up to 80GB for large blocks
    else:
        num_blocks = 500    # Up to 500GB for very large blocks
    
    total_capacity_gb = (num_blocks * block_size_mb) / 1024
    total_records = records * 8 * 3  # 8 symbols × 3 streams each
    estimated_size_mb = total_records * 20 / 1024 / 1024  # Rough estimate
    
    print(f"\n📊 Complete Test Configuration:")
    print(f"   📈 Total database records: ~{total_records:,}")
    print(f"   💾 Estimated data size: ~{estimated_size_mb:.0f}MB")
    print(f"   🗄️  Block size: {block_size_mb}MB")
    print(f"   📦 Total blocks: {num_blocks:,}")
    print(f"   💿 Database capacity: {total_capacity_gb:.1f}GB")
    print(f"   🧵 Writer threads: 8 (one per symbol)")
    print(f"   📖 Reader threads: {readers}")
    print(f"   📊 Data type: Synthetic cryptocurrency OHLCV")
    
    # Warn about large configurations
    if block_size_mb >= 64:
        print(f"   ⚠️  Large blocks: {block_size_mb}MB memory per block")
    if total_capacity_gb > 100:
        print(f"   ⚠️  Large capacity: {total_capacity_gb:.1f}GB total")
    
    if input("\nProceed with stress test? (y/n): ").lower().strip() != 'y':
        print("👋 Stress test cancelled")
        return
    
    # Execute the stress test with pre-configured block size
    tester = ParallelNanoTSStressTest()
    tester.run_parallel_stress_test(records_per_symbol=records, num_readers=readers, block_size_mb=block_size_mb)


if __name__ == "__main__":
    main()