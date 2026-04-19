Đây là repo có tự động hóa CI/CD, bạn push lên github và các image sẽ tự động build, nhưng mà cần lưu ý luồng làm việc của các layer

Trước tiên bạn cần có liên kết với cloud gcp của Tài, làm theo các bước sau: 

  +B1 :  truy cập: https://docs.cloud.google.com/sdk/docs/install-sdk và tải file .exe về máy và chạy để tải (Trong lúc cài, nhớ tích chọn "Install Kubernetes CLI (kubectl)" nếu được hỏi.)
  
  +B2: mở cmd chạy: gcloud auth login   (Lúc này trình duyệt web sẽ bật lên. Đăng nhập bằng gmail .)
  
  +B3: mở cmd chạy: gcloud components install kubectl 
  
  +B4: gửi gmail cho admin để admin cấp quyền
  
  +B5: mở cmd chạy: gcloud container clusters get-credentials bigdata-cluster --region asia-southeast1 --project bigdata-k8s-project
  
  +B6 chạy: gcloud auth configure-docker asia-southeast1-docker.pkg.dev (cho phép docker giao tiếp với cloud ở gg )
  
Quy tắc khi đóng góp vào repo: 

- Đối với crawler:    Sửa code trong thư mục crawler   -> push lên github -> Đợi github báo xanh -> Mở cmd và chạy: kubectl rollout restart deployment crawler-deployment      (Pod sẽ khởi động lại)
- Đối với streaming:  Sửa code trong thư mục streaming -> push lên github -> Đợi github báo xanh -> Mở cmd và chạy: kubectl rollout restart deployment weather-spark-streaming (Pod sẽ khởi động lại)
- Đối với batch:      Sửa code trong thư mục batch     -> push lên github -> Đợi github báo xanh
- Đối với serving:    Sửa code trong thư mục serving   -> push lên github -> Đợi github báo xanh -> Mở cmd và chạy: kubectl rollout restart deployment serving-deployment (Pod sẽ khởi động lại)
- Không tự ý sửa file Dockerfile, file *.yaml (nếu sửa thì cần báo lại trước khi push)

Cách xem giao diện của các pod: Do k để chế độ LoadBalancer nên sẽ k có external IP để truy cập(do mất tiền) , nên mình sẽ port forwarding để truy cập:
- Kibana:  kubectl port-forward svc/kibana 5601:5601
- Minio:   kubectl port-forward svc/minio 9001:9001
- airflow: kubectl port-forward svc/airflow 8080:8080
- serving: kubectl port-forward svc/serving-service 8000:8000
- 
(sau đó truy cập vào http://localhost:5601) or 9001 or 8080 or 8000 

*Note: K nên tự ý sửa lại schema hay đường dẫn tránh làm hỏng cả hệ thống. Nếu có lỗi gì báo lại Tài
