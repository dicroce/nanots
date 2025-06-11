#!/usr/bin/env python3
"""
NanoTS Stress Test - High Volume Cryptocurrency Data

This script downloads and stores massive amounts of real crypto data to test nanots performance:
- Multiple cryptocurrency pairs (BTC, ETH, BNB, ADA, etc.)
- Extended time ranges (weeks/months of data)
- High-frequency data (1-minute intervals)
- Performance benchmarks and analysis

Let's see what nanots can really do!
"""

import datetime
import struct
import time
import csv
from pathlib import Path
import nanots

# Import binance data downloader
try:
    from binance_historical_data import BinanceDataDumper
    BINANCE_AVAILABLE = True
except ImportError:
    BINANCE_AVAILABLE = False
    print("❌ binance-historical-data not installed.")
    exit(1)


class NanoTSStressTester:
    """Comprehensive stress testing for NanoTS with real crypto data."""
    
    def __init__(self):
        self.symbols = [
            "BTCUSDT",   # Bitcoin
            "ETHUSDT",   # Ethereum  
            "BNBUSDT",   # Binance Coin
            "ADAUSDT",   # Cardano
            "SOLUSDT",   # Solana
            "XRPUSDT",   # Ripple
            "DOTUSDT",   # Polkadot
            "LINKUSDT",  # Chainlink
        ]
        self.db_path = "crypto_stress_test.nanots"
        self.data_dir = "./stress_test_data"
        self.start_time = None
        self.stats = {
            'total_records': 0,
            'total_symbols': 0,
            'download_time': 0,
            'storage_time': 0,
            'query_time': 0,
            'db_size_mb': 0
        }
    
    def create_massive_database(self):
        """Create a large NanoTS database optimized for massive crypto data."""
        print("🏗️  Creating massive crypto database...")
        
        # Scale up: 10GB database with 1MB blocks
        block_size = 1024 * 1024  # 1MB blocks
        num_blocks = 10000        # 10GB total capacity
        
        print(f"📊 Database specs:")
        print(f"   Block size: {block_size//1024//1024}MB")
        print(f"   Total blocks: {num_blocks:,}")
        print(f"   Total capacity: {num_blocks * block_size // 1024**3}GB")
        
        nanots.allocate_file(self.db_path, block_size, num_blocks)
        print(f"✅ Allocated {num_blocks:,} blocks of {block_size//1024//1024}MB each")
    
    def download_massive_dataset(self, days_back=30):
        """Download lots of data: multiple symbols over extended time period."""
        print(f"\n📥 MASSIVE DATA DOWNLOAD - {len(self.symbols)} symbols × {days_back} days")
        print("=" * 70)
        
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days_back)
        
        print(f"🎯 Symbols: {', '.join(self.symbols)}")
        print(f"📅 Date range: {start_date} to {end_date} ({days_back} days)")
        print(f"⏱️  Expected records: ~{len(self.symbols) * days_back * 1440:,} (1-minute data)")
        
        download_start = time.time()
        
        for i, symbol in enumerate(self.symbols, 1):
            print(f"\n📦 [{i}/{len(self.symbols)}] Downloading {symbol}...")
            
            try:
                data_dumper = BinanceDataDumper(
                    path_dir_where_to_dump=self.data_dir,
                    asset_class="spot",
                    data_type="klines", 
                    data_frequency="1m",
                )
                
                data_dumper.dump_data(
                    tickers=[symbol],
                    date_start=start_date,
                    date_end=end_date,
                    is_to_update_existing=True
                )
                
                # Quick validation
                csv_files = list(Path(self.data_dir).rglob(f"*{symbol}*.csv"))
                total_size = sum(f.stat().st_size for f in csv_files) / 1024 / 1024
                print(f"   ✅ {symbol}: {len(csv_files)} files, {total_size:.1f}MB")
                
            except Exception as e:
                print(f"   ❌ {symbol} download failed: {e}")
                continue
        
        download_time = time.time() - download_start
        self.stats['download_time'] = download_time
        
        print(f"\n🎉 Download completed in {download_time:.1f} seconds")
        
        # Count total downloaded files
        all_csv_files = list(Path(self.data_dir).rglob("*.csv"))
        total_size_gb = sum(f.stat().st_size for f in all_csv_files) / 1024**3
        print(f"📊 Total downloaded: {len(all_csv_files)} CSV files, {total_size_gb:.2f}GB")
    
    def parse_csv_fast(self, csv_file):
        """Optimized CSV parsing for maximum speed."""
        records = []
        
        try:
            with open(csv_file, 'r') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    if len(row) < 12:
                        continue
                    
                    try:
                        # Fast parsing - minimal validation for speed
                        open_time_us = int(float(row[0]))
                        open_time_ms = open_time_us // 1000  # Convert to milliseconds
                        
                        # Quick timestamp sanity check
                        if open_time_ms < 1609459200000 or open_time_ms > 1924992000000:  # 2021-2030
                            continue
                        
                        records.append({
                            'timestamp': open_time_ms,
                            'open': float(row[1]),
                            'high': float(row[2]),
                            'low': float(row[3]),
                            'close': float(row[4]),
                            'volume': float(row[5]),
                            'trades': int(float(row[8])),
                            'taker_base': float(row[9]),
                            'taker_quote': float(row[10])
                        })
                    except (ValueError, IndexError):
                        continue
            
            # Sort for nanots monotonic requirement
            records.sort(key=lambda x: x['timestamp'])
            return records
            
        except Exception as e:
            print(f"❌ Error parsing {csv_file}: {e}")
            return []
    
    def store_massive_data(self):
        """Store all downloaded data in nanots with performance monitoring."""
        print(f"\n💾 MASSIVE DATA STORAGE")
        print("=" * 50)
        
        storage_start = time.time()
        writer = nanots.Writer(self.db_path, auto_reclaim=True)
        
        # Create contexts for each symbol
        contexts = {}
        for symbol in self.symbols:
            contexts[f"{symbol}_price"] = writer.create_context(
                f"{symbol}_price", f"OHLCV data for {symbol}")
            contexts[f"{symbol}_volume"] = writer.create_context(
                f"{symbol}_volume", f"Volume data for {symbol}")
            contexts[f"{symbol}_trades"] = writer.create_context(
                f"{symbol}_trades", f"Trade data for {symbol}")
        
        total_stored = 0
        files_processed = 0
        
        # Process all CSV files
        all_csv_files = list(Path(self.data_dir).rglob("*.csv"))
        print(f"🎯 Processing {len(all_csv_files)} CSV files...")
        
        for i, csv_file in enumerate(all_csv_files, 1):
            # Extract symbol from filename
            symbol = None
            for sym in self.symbols:
                if sym in csv_file.name:
                    symbol = sym
                    break
            
            if not symbol:
                continue
                
            print(f"📊 [{i}/{len(all_csv_files)}] Processing {csv_file.name}...", end="")
            
            records = self.parse_csv_fast(csv_file)
            if not records:
                print(" ⚠️  No valid records")
                continue
            
            # Bulk write to nanots
            records_written = 0
            for record in records:
                try:
                    timestamp = record['timestamp']
                    
                    # Price data (OHLC)
                    price_data = struct.pack('ffff', 
                                           record['open'], record['high'], 
                                           record['low'], record['close'])
                    writer.write(contexts[f"{symbol}_price"], price_data, timestamp, 0)
                    
                    # Volume data
                    volume_data = struct.pack('f', record['volume'])
                    writer.write(contexts[f"{symbol}_volume"], volume_data, timestamp, 0)
                    
                    # Trade data
                    trade_data = struct.pack('iff', record['trades'], 
                                           record['taker_base'], record['taker_quote'])
                    writer.write(contexts[f"{symbol}_trades"], trade_data, timestamp, 0)
                    
                    records_written += 1
                    
                except Exception as e:
                    continue  # Skip invalid records for speed
            
            total_stored += records_written
            files_processed += 1
            
            print(f" ✅ {records_written:,} records")
            
            # Progress update every 50 files
            if files_processed % 50 == 0:
                elapsed = time.time() - storage_start
                rate = total_stored / elapsed
                print(f"   📈 Progress: {total_stored:,} records in {elapsed:.1f}s ({rate:.0f} records/sec)")
        
        storage_time = time.time() - storage_start
        self.stats['storage_time'] = storage_time
        self.stats['total_records'] = total_stored
        self.stats['total_symbols'] = len([s for s in self.symbols if any(s in f.name for f in all_csv_files)])
        
        print(f"\n🎉 STORAGE COMPLETE!")
        print(f"   📊 Total records stored: {total_stored:,}")
        print(f"   ⏱️  Storage time: {storage_time:.1f} seconds")
        print(f"   🚀 Storage rate: {total_stored/storage_time:.0f} records/second")
    
    def performance_benchmarks(self):
        """Run comprehensive performance benchmarks."""
        print(f"\n⚡ PERFORMANCE BENCHMARKS")
        print("=" * 50)
        
        benchmark_start = time.time()
        
        # Database size
        db_size = Path(self.db_path).stat().st_size / 1024 / 1024
        self.stats['db_size_mb'] = db_size
        print(f"💾 Database size: {db_size:.1f} MB")
        
        benchmarks = []
        
        for symbol in self.symbols[:3]:  # Test first 3 symbols
            print(f"\n🔥 Benchmarking {symbol}...")
            
            try:
                # Test 1: Iterator creation speed
                start = time.time()
                iterator = nanots.Iterator(self.db_path, f"{symbol}_price")
                iter_time = time.time() - start
                
                if not iterator.valid():
                    print(f"   ⚠️  No data for {symbol}")
                    continue
                
                # Test 2: Sequential read speed
                start = time.time()
                count = 0
                for frame in iterator:
                    count += 1
                    if count >= 10000:  # Read 10k records
                        break
                seq_time = time.time() - start
                seq_rate = count / seq_time if seq_time > 0 else 0
                
                # Test 3: Random access speed  
                iterator.reset()
                reader = nanots.Reader(self.db_path)
                
                # Get time range
                first_frame = iterator.get_current_frame()
                start_ts = first_frame['timestamp']
                
                for _ in range(100):  # Skip ahead
                    if iterator.valid():
                        iterator.next()
                end_frame = iterator.get_current_frame()
                end_ts = end_frame['timestamp'] if iterator.valid() else start_ts + 100000
                
                # Query time range
                start = time.time()
                frames = reader.read(f"{symbol}_price", start_ts, end_ts)
                query_time = time.time() - start
                query_rate = len(frames) / query_time if query_time > 0 else 0
                
                benchmarks.append({
                    'symbol': symbol,
                    'iterator_time': iter_time * 1000,  # ms
                    'sequential_rate': seq_rate,
                    'query_time': query_time * 1000,   # ms
                    'query_rate': query_rate,
                    'query_records': len(frames)
                })
                
                print(f"   🏃 Sequential: {seq_rate:.0f} records/sec")
                print(f"   🎯 Query: {len(frames)} records in {query_time*1000:.1f}ms ({query_rate:.0f} records/sec)")
                
            except Exception as e:
                print(f"   ❌ Benchmark failed: {e}")
        
        self.stats['query_time'] = sum(b['query_time'] for b in benchmarks) / len(benchmarks) if benchmarks else 0
        
        # Summary
        print(f"\n📊 PERFORMANCE SUMMARY:")
        if benchmarks:
            avg_seq_rate = sum(b['sequential_rate'] for b in benchmarks) / len(benchmarks)
            avg_query_rate = sum(b['query_rate'] for b in benchmarks) / len(benchmarks)
            print(f"   🏃 Average sequential read: {avg_seq_rate:.0f} records/sec")
            print(f"   🎯 Average query speed: {avg_query_rate:.0f} records/sec")
        
        benchmark_time = time.time() - benchmark_start
        print(f"   ⏱️  Benchmark time: {benchmark_time:.1f} seconds")
    
    def final_analysis(self):
        """Comprehensive analysis of the stress test results."""
        print(f"\n🎊 STRESS TEST RESULTS")
        print("=" * 60)
        
        print(f"📊 DATA SCALE:")
        print(f"   💾 Database size: {self.stats['db_size_mb']:.1f} MB")
        print(f"   📈 Total records: {self.stats['total_records']:,}")
        print(f"   🪙 Symbols tested: {self.stats['total_symbols']}")
        print(f"   📅 Data density: {self.stats['total_records']/self.stats['db_size_mb']:.1f} records/MB")
        
        print(f"\n⚡ PERFORMANCE:")
        print(f"   📥 Download speed: {self.stats['download_time']:.1f} seconds")
        print(f"   💾 Storage speed: {self.stats['total_records']/self.stats['storage_time']:.0f} records/sec")
        print(f"   🎯 Query speed: {self.stats['query_time']:.1f}ms average")
        
        # Calculate theoretical limits
        total_time = self.stats['download_time'] + self.stats['storage_time']
        print(f"\n🚀 THEORETICAL CAPACITY:")
        print(f"   📊 Processing rate: {self.stats['total_records']/total_time:.0f} records/sec end-to-end")
        
        # Estimate daily crypto market capacity
        daily_minutes = 24 * 60
        daily_capacity = (self.stats['total_records'] / total_time) * daily_minutes
        print(f"   📈 Daily market capacity: ~{daily_capacity:,.0f} records/day")
        print(f"   🌍 Crypto pairs supportable: ~{daily_capacity/(daily_minutes):.0f} pairs at 1-minute freq")
        
        print(f"\n🎯 STRESS TEST VERDICT:")
        if self.stats['total_records'] > 100000:
            print(f"   🏆 EXCELLENT: NanoTS handled {self.stats['total_records']:,} records like a champion!")
        elif self.stats['total_records'] > 50000:
            print(f"   ✅ GOOD: NanoTS processed {self.stats['total_records']:,} records successfully")
        else:
            print(f"   ⚠️  MODERATE: {self.stats['total_records']:,} records processed")
        
        # Keep files decision
        print(f"\n📂 Database file: {self.db_path} ({self.stats['db_size_mb']:.1f}MB)")
    
    def run_stress_test(self, days_back=30):
        """Run the complete stress test suite."""
        print("🔥 NANOTS STRESS TEST - LET'S SEE WHAT IT CAN DO!")
        print("=" * 70)
        
        self.start_time = time.time()
        
        try:
            # Step 1: Create massive database
            self.create_massive_database()
            
            # Step 2: Download tons of data
            self.download_massive_dataset(days_back)
            
            # Step 3: Store everything in nanots
            self.store_massive_data()
            
            # Step 4: Performance benchmarks
            self.performance_benchmarks()
            
            # Step 5: Final analysis
            self.final_analysis()
            
            total_time = time.time() - self.start_time
            print(f"\n🏁 STRESS TEST COMPLETED in {total_time:.1f} seconds")
            
        except KeyboardInterrupt:
            print(f"\n⚠️  Stress test interrupted by user")
        except Exception as e:
            print(f"\n❌ Stress test failed: {e}")
            raise
        
        # Cleanup option
        keep = input(f"\n🗑️  Keep {self.db_path} ({self.stats['db_size_mb']:.1f}MB)? (y/n): ").lower().strip()
        if keep != 'y':
            if Path(self.db_path).exists():
                Path(self.db_path).unlink()
                print(f"🗑️  Removed {self.db_path}")
            
            import shutil
            if Path(self.data_dir).exists():
                shutil.rmtree(self.data_dir)
                print(f"🗑️  Removed {self.data_dir}/")


def main():
    """Main stress test execution."""
    print("🚀 Welcome to the NanoTS Stress Test!")
    print("This will download and process MASSIVE amounts of crypto data.")
    print()
    
    # User configuration
    print("📊 Stress test options:")
    print("  1. 🔥 INTENSE: 8 symbols × 30 days (~345K records)")
    print("  2. 🚀 EXTREME: 8 symbols × 90 days (~1M records)")  
    print("  3. 💀 INSANE: 8 symbols × 180 days (~2M records)")
    print("  4. 🎯 Custom")
    
    choice = input("\nSelect test level (1-4): ").strip()
    
    if choice == "1":
        days = 30
        print("🔥 INTENSE mode selected!")
    elif choice == "2":
        days = 90
        print("🚀 EXTREME mode selected!")
    elif choice == "3":
        days = 180
        print("💀 INSANE mode selected! This will take a while...")
    elif choice == "4":
        days = int(input("Enter days of data: "))
        print(f"🎯 Custom: {days} days selected")
    else:
        days = 30
        print("🔥 Default INTENSE mode")
    
    print(f"\n⚠️  This will download ~{days * 8 * 1440:,} records ({days * 8 * 1440 * 32 / 1024 / 1024:.1f}MB estimated)")
    
    if input("Continue? (y/n): ").lower().strip() != 'y':
        print("👋 Stress test cancelled")
        return
    
    # Run the stress test
    tester = NanoTSStressTester()
    tester.run_stress_test(days_back=days)


if __name__ == "__main__":
    main()
