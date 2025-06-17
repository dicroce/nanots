"""

import os
import tempfile
import time
import json
import nanots


def nanots_basics_example():
    # 1. CREATE DATABASE
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nanots') as tmp:
        db_file = tmp.name
    
    try:
        # Allocate database: 64KB blocks, 100 blocks = ~6MB
        nanots.allocate_file(db_file, 64*1024, 100)
        
        # 2. WRITE DATA
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
        
        # 3. READ DATA
        reader = nanots.Reader(db_file)
        
        # Read all data
        frames = reader.read("sensors", base_time, base_time + 50000)
        
        # Show first 3 records
        for i, frame in enumerate(frames[:3]):
            data = json.loads(frame['data'].decode('utf-8'))
            print(f"      {i+1}. {data['temp_c']}°C from {data['sensor']}")
        
        # 4. ITERATE THROUGH DATA
        iterator = nanots.Iterator(db_file, "sensors")
        
        count = 0
        total_temp = 0
        
        for frame in iterator:
            data = json.loads(frame['data'].decode('utf-8'))
            total_temp += data['temp_c']
            count += 1
        
        avg_temp = total_temp / count if count > 0 else 0
        
        # 5. DISCOVER STREAMS
        streams = reader.query_stream_tags(base_time, base_time + 50000)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        # Cleanup
        if os.path.exists(db_file):
            os.unlink(db_file)


if __name__ == "__main__":
    nanots_basics_example()

"""


# nanots Python bindings

# Open x64 Native Console

# You gotta have cython and build installed

pip install cython
pip install build

# on linux, build with make, on windows, "python -m build"