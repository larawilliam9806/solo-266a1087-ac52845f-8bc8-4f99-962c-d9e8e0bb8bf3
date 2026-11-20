#!/bin/sh
# 单 worker + 多线程：SQLite 写事务（BEGIN IMMEDIATE）天然串行化，
# 单进程避免跨进程锁竞争；换参考的重验计算在事务外分批进行，重验期间
# 其他线程仍可受理样本上传/重传/删除与页面读取。
# timeout 放宽到 600s：重验大批样本的换参考请求本身是同步处理的，
# 而重验不会持有写锁，其他请求不受影响。
set -e

mkdir -p "$(dirname "$DATABASE_PATH")"

exec gunicorn --bind 0.0.0.0:8000 \
    --workers 1 \
    --threads 8 \
    --timeout 600 \
    --access-logfile - \
    --error-logfile - \
    "app:create_app()"
