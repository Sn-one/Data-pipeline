# schemas.py
# Schema for accelerometer data
accelerometer_schema = StructType([
    StructField("messageId", IntegerType()),
    StructField("sessionId", StringType()),
    StructField("deviceId", StringType()),
    StructField("sensorName", StringType()),
    StructField("time", TimestampType()),
    StructField("accuracy", IntegerType()),
    StructField("x", FloatType()),
    StructField("y", FloatType()),
    StructField("z", FloatType())
])

# Schema for orientation data
orientation_schema = StructType([
    StructField("messageId", IntegerType()),
    StructField("sessionId", StringType()),
    StructField("deviceId", StringType()),
    StructField("sensorName", StringType()),
    StructField("time", TimestampType()),
    StructField("accuracy", IntegerType()),
    StructField("qx", FloatType()),
    StructField("qy", FloatType()),
    StructField("qz", FloatType()),
    StructField("qw", FloatType()),
    StructField("roll", FloatType()),
    StructField("pitch", FloatType()),
    StructField("yaw", FloatType())
])

# Schema for magnetometer data
magnetometer_schema = StructType([
    StructField("messageId", IntegerType()),
    StructField("sessionId", StringType()),
    StructField("deviceId", StringType()),
    StructField("sensorName", StringType()),
    StructField("time", TimestampType()),
    StructField("accuracy", IntegerType()),
    StructField("x", FloatType()),
    StructField("y", FloatType()),
    StructField("z", FloatType())
])

# Schema for gyroscope data
gyroscope_schema = StructType([
    StructField("messageId", IntegerType()),
    StructField("sessionId", StringType()),
    StructField("deviceId", StringType()),
    StructField("sensorName", StringType()),
    StructField("time", TimestampType()),
    StructField("accuracy", IntegerType()),
    StructField("x", FloatType()),
    StructField("y", FloatType()),
    StructField("z", FloatType())
])

# Schema for gravity data
gravity_schema = StructType([
    StructField("messageId", IntegerType()),
    StructField("sessionId", StringType()),
    StructField("deviceId", StringType()),
    StructField("sensorName", StringType()),
    StructField("time", TimestampType()),
    StructField("accuracy", IntegerType()),
    StructField("x", FloatType()),
    StructField("y", FloatType()),
    StructField("z", FloatType())
])
