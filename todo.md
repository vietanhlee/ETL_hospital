Tôi muốn chuyển project ETL mysql DW này thành project ETL dữ liệu vào ClickHouse DW (hiện tại là dùng mysql DW), nhưng vẫn giữ MySQL làm nguồn dữ liệu đầu vào.

1. Đọc file script_final/load_hdfs_to_mysql để lấy thông tin các bảng, các trường trong đó kết hợp với các file sql trong thư mục sql tạo bảng được cung cấp từ đó lấy ra được các bảng, các trường, các kiểu dữ liệu.
2. Cho tôi code sql để tạo các bảng trong clickhouse DW, bao gồm tên bảng, tên trường, kiểu dữ liệu tương ứng, bạn có thể thay đổi kiểu dữ liệu nếu cần thiết để phù hợp với clickhouse. Chú ý là chỗ tạo bảng của mysql nó đang dùng các khoá để ràng buộc, tuy nhiên làm DW thì không cần thiết phải có khoá chính, khoá ngoại. Cái đó phụ thuộc vào code nghiệp vụ ETL để đảm bảo tính nhất quán dữ liệu, chứ không phải phụ thuộc vào ràng buộc của database. Nên bạn có thể bỏ qua phần khoá chính, khoá ngoại khi tạo bảng trong clickhouse nhé, chỉ cần tạo bảng với các trường và kiểu dữ liệu thôi.
3. Chuyển toàn bộ code của project này sang clickhouse DW, mọi thứ không còn liên quan đến mysql nữa nhé, nhưng vẫn giữ nguyên cấu trúc thư mục, tên file, logic nghiệp vụ. Nói chung là chỉ thay mỗi cái là thay vì đổ vào mysql thì đổ vào clickhouse thôi, còn lại mọi thứ vẫn giữ nguyên nhé. Các tên file thì bạn thay đổi nếu cần thiết để phù hợp với clickhouse, nhưng vẫn giữ nguyên logic nghiệp vụ và cấu trúc thư mục.
4. Tạo tôi file .env để config các host, pass, port các thứ, ... và lệnh docker-compose để chạy toàn project nữa nhé. Cho tôi readme để hướng dẫn cài, config và chạy.
Ở clickhouse, tôi dùng cloud host và có đoạn code hướng dẫn connect như sau:

import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='qlfb8ypu5w.ap-northeast-1.aws.clickhouse.cloud',
        user='default',
        password='N7f8bLl.qrbON',
        secure=True
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])
Bạn dựa vào và convert cho phù hợp nhé. 

(Cái file .jar của mysql tôi không biết là làm gì nữa, vào file .sh thi thấy nó dùng cho spark submit, nhưng mà giờ chuyển sang clickhouse thì không cần nữa đúng không, hay là vẫn cần để chạy spark submit nhưng thay vì đổ vào mysql thì đổ vào clickhouse, bạn xem lại và cho tôi biết nhé, nếu cần thì giữ lại. Không cần lệnh build container clickhouse đâu, tôi dùng cloud host rồi)
(bạn cần cmt code và trả lời tôi bằng tiếng việt có dấu đàng hoàng nhé)
