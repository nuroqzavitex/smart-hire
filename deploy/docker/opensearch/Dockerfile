# OpenSearch for the job read model (ADR 0009): the official image plus the
# analysis-icu plugin (icu_tokenizer, icu_folding) for Vietnamese search.
FROM opensearchproject/opensearch:2.19.2
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch analysis-icu
