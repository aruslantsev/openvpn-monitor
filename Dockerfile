FROM python:3.8
COPY requirements.txt /app/requirements.txt
RUN apt update \
    && apt install -y dumb-init \
    && apt clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache -r /app/requirements.txt

COPY . .

ENV PYTHONPATH=.
ENTRYPOINT ["dumb-init"] 
CMD ["python", "-u", "openvpn_monitor/jobs/run.py"]
EXPOSE 8888
