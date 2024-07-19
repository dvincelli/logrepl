FROM python:3.12-alpine

RUN apk add --no-cache postgresql15-client

RUN addgroup -g 1000 -S app && adduser -u 1000 -S app -G app -h /app

COPY pdm.lock pyproject.toml /app/

COPY logrepl /app/logrepl
RUN chown -R 1000:1000 /app

USER 1000:1000

ENV HOME=/app \
    PATH=/app/.local/bin:$PATH

WORKDIR /app

RUN pip install --user pdm --no-cache-dir
RUN pdm install --prod

ENTRYPOINT ["pdm", "run"]

CMD ["python", "-m", "logrepl"]
