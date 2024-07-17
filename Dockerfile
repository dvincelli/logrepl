FROM python:3.12

COPY . /app
WORKDIR /app

RUN pip install pdm
RUN pdm install

ENTRYPOINT ["pdm", "run"]

CMD ["python", "-m", "logrepl"]
