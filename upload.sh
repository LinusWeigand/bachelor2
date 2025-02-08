export IP=3.68.183.56
ssh -i ~/.ssh/mvp-key-pair-server.pem ec2-user@$IP
scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/version3 ec2-user@$IP:/home/ec2-user
scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/bufreader ec2-user@$IP:/home/ec2-user
scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/memmap ec2-user@$IP:/home/ec2-user
scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/memmap2 ec2-user@$IP:/home/ec2-user
iostat -dx 1
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches

scp -i ~/.ssh/mvp-key-pair-server.pem -r ./snowset-main.parquet/ ec2-user@$IP:/mnt/raid0/

scp -i ~/.ssh/mvp-key-pair-server.pem ./max ec2-user@$IP:/home/ec2-user/

scp -i ~/.ssh/mvp-key-pair-server.pem ./snowset-main.parquet/merged_*.parquet ec2-user@$IP:/mnt/raid0/

scp -i ~/.ssh/mvp-key-pair-server.pem ./snowset-main.parquet/merged_01.parquet ec2-user@$IP:/mnt/raid0/

scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/seqread ec2-user@$IP:/home/ec2-user
scp -i ~/.ssh/mvp-key-pair-server.pem ./target/x86_64-unknown-linux-gnu/release/parquet2 ec2-user@$IP:/home/ec2-user

scp -i ~/.ssh/mvp-key-pair-server.pem ec2-user@$IP:/home/ec2-user/parquet2.svg "/Users/linusweigand/Documents/CodeProjects/rust/Bachelor/row-group-skipper/flamegraphs/parquet2.svg"
