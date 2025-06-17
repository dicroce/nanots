```

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
            
            print("Writing data:", data.decode('utf-8'), "at", timestamp)
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

        iterator.find(base_time + 10000)

        print("Iterating through data:")
        for i, frame in enumerate(iterator):
            data = json.loads(frame['data'].decode('utf-8'))
            print(f"      {i+1}. {data['temp_c']}°C from {data['sensor']} at {frame['timestamp']}")
                
        # 5. DISCOVER STREAMS
        streams = reader.query_stream_tags(base_time, base_time + 50000)
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    nanots_basics_example()


```
