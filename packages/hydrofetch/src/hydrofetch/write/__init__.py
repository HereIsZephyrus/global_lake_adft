"""Output writers: persist sampled lake-forcing data to file or DB sinks.

Concrete writers:
    :class:`~hydrofetch.write.file_writer.FileWriter`  – Parquet / CSV files
    :class:`~hydrofetch.write.db_writer.DBWriter`      – PostgreSQL upsert
    :class:`~hydrofetch.write.pipeline_writer.PipelineWriter` – fan-out to multiple sinks

Use :func:`~hydrofetch.write.factory.get_writer` to build the appropriate
writer from a :class:`~hydrofetch.jobs.models.WriteParams` instance.
"""
