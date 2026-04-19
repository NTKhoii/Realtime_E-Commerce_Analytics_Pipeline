FROM apache/spark:3.5.7-java17-python3

USER root

# Cài đặt Python 3.12 để đồng bộ với cụm NodeManager
RUN apt-get update && apt-get install -y software-properties-common wget zip curl python-is-python3 \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update \
    && apt-get install -y python3.12 python3.12-venv \
    && apt-get autoremove -y && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tạo thư mục và cấp quyền
RUN mkdir -p /data && chown -R spark:root /data \
    && mkdir -p /home/spark && chown -R spark:spark /home/spark && usermod -d /home/spark spark \
    && mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2

VOLUME /data
VOLUME /home/spark/.ivy2

# Cài đặt công cụ uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER spark
WORKDIR /home/spark

COPY --chown=spark:spark requirements.txt /tmp/requirements.txt

ENV VIRTUAL_ENV="/home/spark/.venv"
ENV PATH="/home/spark/.venv/bin:/opt/spark/bin:/opt/spark/sbin:$PATH"

# Ép uv dùng Python 3.12 để tạo môi trường
RUN uv venv --python /usr/bin/python3.12 \
    && uv pip install -r /tmp/requirements.txt \
    && venv-pack -o /home/spark/pyspark_venv.tar.gz \
    && rm /tmp/requirements.txt

EXPOSE 4040 4041