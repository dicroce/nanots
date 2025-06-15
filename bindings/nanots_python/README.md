"""

import os
import tempfile
import time
import json
import nanots


def nanots_basics_example():
    """Quick demo of NanoTS core functionality."""
    
    print("🚀 NanoTS Python Basics")
    print("=" * 30)
    
    # 1. CREATE DATABASE
    print("📁 Creating database...")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nanots') as tmp:
        db_file = tmp.name
    
    try:
        # Allocate database: 64KB blocks, 100 blocks = ~6MB
        nanots.allocate_file(db_file, 64*1024, 100)
        print("   ✅ Database allocated")
        
        # 2. WRITE DATA
        print("\n✍️  Writing data...")
        writer = nanots.Writer(db_file)
        context = writer.create_context("sensors", "Temperature readings")
        
        # Write 10 temperature readings
        base_time = int(time.time() * 1000)
        for i in range(10):
            timestamp = base_time + (i * 5000)  # Every 5 seconds
            temperature = 20.0 + i + (i * 0.5)  # Increasing temp
            
            data = json.dumps({
                "temp_c": temperature,
                "sensor": "temp_01"
            }).encode('utf-8')
            
            writer.write(context, data, timestamp, 0)
        
        print(f"   ✅ Wrote 10 temperature readings")
        
        # 3. READ DATA
        print("\n📖 Reading data...")
        reader = nanots.Reader(db_file)
        
        # Read all data
        frames = reader.read("sensors", base_time, base_time + 50000)
        print(f"   ✅ Read {len(frames)} records")
        
        # Show first 3 records
        print("\n   📊 Sample data:")
        for i, frame in enumerate(frames[:3]):
            data = json.loads(frame['data'].decode('utf-8'))
            print(f"      {i+1}. {data['temp_c']}°C from {data['sensor']}")
        
        # 4. ITERATE THROUGH DATA
        print("\n🔄 Using iterator...")
        iterator = nanots.Iterator(db_file, "sensors")
        
        count = 0
        total_temp = 0
        
        for frame in iterator:
            data = json.loads(frame['data'].decode('utf-8'))
            total_temp += data['temp_c']
            count += 1
        
        avg_temp = total_temp / count if count > 0 else 0
        print(f"   ✅ Processed {count} records")
        print(f"   📊 Average temperature: {avg_temp:.1f}°C")
        
        # 5. DISCOVER STREAMS
        print("\n🔍 Discovering streams...")
        streams = reader.query_stream_tags(base_time, base_time + 50000)
        print(f"   ✅ Found streams: {streams}")
        
        print("\n🎉 NanoTS basics complete!")
        print("\n💡 Key concepts:")
        print("   • allocate_file() - creates database")
        print("   • Writer/context - writes timestamped data")
        print("   • Reader - queries time ranges")
        print("   • Iterator - sequential data access")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        # Cleanup
        if os.path.exists(db_file):
            os.unlink(db_file)
            print(f"\n🧹 Cleaned up {os.path.basename(db_file)}")


if __name__ == "__main__":
    nanots_basics_example()

"""


# nanots Python bindings

# Open x64 Native Console

# You gotta have cython and build installed

pip install cython
pip install build

# on linux, build with make, on windows, "python -m build"