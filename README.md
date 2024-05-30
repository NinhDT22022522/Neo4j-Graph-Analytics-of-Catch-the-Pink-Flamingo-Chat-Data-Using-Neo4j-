# Neo4j-Bài tập lớn của nhóm 4

## Giới thiệu

Đây là bài tập lớn của nhóm 4 cho khóa học về Phân tích dữ liệu lớn với dữ liệu chat sử dụng Neo4j. Trong bài tập này, chúng tôi sẽ sử dụng Neo4j để phân tích các dữ liệu chat và tìm ra các thông tin quan trọng từ các tương tác giữa người dùng. Dự án này được thực hiện bởi các thành viên:

- 22022522 Đàm Thái Ninh
- 22022653 Long Trí Thái Sơn
- 22022548 Hoàng Đăng Khoa

## Đề bài

Bạn có thể tìm hiểu đề bài chi tiết tại [đây](https://github.com/AlessandroCorradini/University-of-California-San-Diego-Big-Data-Specialization/tree/master/06%20-%20Big%20Data%20-%20Capstone%20Project/04%20-%20Graph%20Analytics%20With%20Chat%20Data%20Using%20Neo4j).

## Cài đặt

### 1. Cài đặt Neo4j Desktop

- Tải và cài đặt [Neo4j Desktop](https://neo4j.com/download/).
- Kéo thả file `script.zip` vào Neo4j Browser để có thể dùng ngay lệnh Cypher.

### 2. Import dữ liệu từ file CSV

- Đưa các dữ liệu từ folder `data` vào thư mục `import` trong Neo4j Data.
- Chi tiết có thể tra cứu trên mạng hoặc tài liệu của Neo4j.

### 3. Sử dụng Neo4j Connector với Spark

#### Giới thiệu

Để tích hợp Neo4j với Apache Spark, chúng tôi sử dụng Neo4j Connector cho Spark. Điều này giúp chúng tôi thực hiện phân tích dữ liệu lớn hơn và phức tạp hơn bằng cách kết hợp khả năng lưu trữ đồ thị của Neo4j với khả năng xử lý dữ liệu mạnh mẽ của Spark.

#### Cài đặt

Thêm dependency sau vào `build.sbt` nếu bạn sử dụng SBT:

```
libraryDependencies += "org.neo4j.spark" %% "neo4j-connector" % "4.0.0"
```
Hoặc nếu bạn sử dụng Maven, thêm đoạn sau vào `pom.xml`:

```
<dependency>
  <groupId>org.neo4j.spark</groupId>
  <artifactId>neo4j-connector</artifactId>
  <version>4.0.0</version>
</dependency>
```

#### Ví dụ sử dụng

Dưới đây là một ví dụ đơn giản về cách sử dụng Neo4j Connector với Spark:

```
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Neo4jConnectorExample")
  .config("spark.neo4j.bolt.url", "bolt://localhost:7687")
  .config("spark.neo4j.bolt.user", "neo4j")
  .config("spark.neo4j.bolt.password", "password")
  .getOrCreate()

// Đọc dữ liệu từ Neo4j
val df = spark.read.format("org.neo4j.spark.DataSource")
  .option("labels", "Person")
  .load()

df.show()

// Ghi dữ liệu vào Neo4j
df.write.format("org.neo4j.spark.DataSource")
  .option("labels", "Person")
  .mode("overwrite")
  .save()
```

### Sử dụng
#### Chạy các truy vấn Cypher
Sử dụng các lệnh Cypher từ file `script.zip` để thực hiện các truy vấn và phân tích dữ liệu.

#### Phân tích dữ liệu
Thực hiện các phân tích dữ liệu bằng cách sử dụng các truy vấn Cypher đã được chuẩn bị sẵn trong script.zip. Bạn có thể mở Neo4j Browser và chạy các truy vấn này để nhận được kết quả.

