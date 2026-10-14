FROM gcr.io/distroless/cc-debian13:nonroot
ARG TARGETARCH
COPY --chmod=755 dist/${TARGETARCH}/clickhouse-promql /usr/local/bin/clickhouse-promql
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/clickhouse-promql"]
CMD ["--host", "0.0.0.0"]
