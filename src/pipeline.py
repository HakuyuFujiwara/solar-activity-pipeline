"""Main pipeline orchestrator.

Coordinates the full ETL workflow: ingest from all sources, transform,
cross-validate, detect anomalies, persist to database, and optionally
export to the legacy .dat format.

Usage:
    python -m src.pipeline --days-back 30
    python -m src.pipeline --start-date 2025-01-01 --end-date 2025-01-31
    python -m src.pipeline --export-dat --run-number 76 --mdi-day-start 5401
    python -m src.pipeline --init-db
    python -m src.pipeline --dry-run --days-back 7
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import structlog

from src.config import settings
from src.ingestion.aavso import AAVSOSource
from src.ingestion.base import IngestionError, SolarObservation
from src.ingestion.noaa import NOAASource
from src.ingestion.silso import SILSOSource
from src.ingestion.spaceweather_ca import SpaceWeatherCASource
from src.ingestion.lasp import LASPSource
from src.ingestion.mgii import MgIISource
from src.run_registry import get_run, LAB_OUTPUT_DIR
from src.processing.anomaly import AnomalyDetector
from src.processing.transformer import Transformer
from src.processing.validator import CrossValidator
from src.storage.database import Database

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


class Pipeline:
    """Orchestrates the solar activity data pipeline.

    Manages the lifecycle of source adapters, runs processing steps in order,
    and handles error recovery. Each run is logged to the database for
    observability.
    """

    def __init__(self, db: Database | None = None, dry_run: bool = False) -> None:
        self.db = db or Database()
        self.dry_run = dry_run
        self.sources = [
            AAVSOSource(),
            NOAASource(),
            SILSOSource(),
            SpaceWeatherCASource(),
            LASPSource(),
            MgIISource(),
        ]
        self.transformer = Transformer()
        self.validator = CrossValidator()
        self.anomaly_detector = AnomalyDetector()

    def run(self, start: date, end: date, export_dat: bool = False, **export_kwargs) -> None:
        """Execute the full pipeline for the given date range.

        Steps:
            1. Create pipeline run record
            2. Ingest from all sources (with graceful degradation)
            3. Cross-validate multi-source data
            4. Transform to unified records
            5. Detect anomalies
            6. Persist everything to database
            7. Optionally export .dat file
        """
        run_id = None
        all_observations: list[SolarObservation] = []

        try:
            # Step 1: Record this pipeline execution
            if not self.dry_run:
                self.db.create_tables()
                run_id = self.db.create_pipeline_run(start, end, dry_run=self.dry_run)

            logger.info("pipeline_start", start=str(start), end=str(end), dry_run=self.dry_run)

            # Step 2: Ingest from all sources
            all_observations = self._ingest_all(start, end)
            if not all_observations:
                logger.warning("pipeline_no_data", msg="No observations from any source")

            # Step 3: Cross-validate
            report = self.validator.validate(all_observations)

            # Step 4: Transform to unified records
            unified = self.transformer.to_unified_records(all_observations)

            # Step 5: Detect anomalies
            anomalies = self.anomaly_detector.detect(unified)

            # Step 6: Persist
            obs_count = 0
            anom_count = 0
            if not self.dry_run:
                obs_count = self.db.upsert_observations(all_observations)
                anom_count = self.db.save_anomalies(anomalies)
            else:
                logger.info("dry_run_skip_persist", observations=len(all_observations))

            # Step 7: Export .dat if requested
            if export_dat and unified:
                dat_path = self.transformer.export_dat(unified, **export_kwargs)
                logger.info("dat_exported", path=dat_path)
                # Save raw source files if requested
            if getattr(self, '_save_sources', False):
                self._save_source_files(start, end, settings.dat_output_dir)

            # Mark success
            if run_id is not None:
                self.db.complete_pipeline_run(
                    run_id,
                    status="success",
                    observations=obs_count,
                    anomalies=anom_count,
                )

            for record in unified:
                d = record["date"]
                sem_last = record.get("sem_last")
                if sem_last:
                    try:
                        val = float(sem_last)
                        if val > 8e10:
                            logger.warning("suspicious_sem_value", date=str(d), value=sem_last)
                    except ValueError:
                        pass

            logger.info(
                "pipeline_complete",
                observations=len(all_observations),
                unified_records=len(unified),
                anomalies=len(anomalies),
                validation_invalid=report.invalid_dates,
            )

        except Exception as exc:
            logger.error("pipeline_failed", error=str(exc))
            if run_id is not None:
                self.db.complete_pipeline_run(run_id, status="failed", error=str(exc))
            raise

        finally:
            for source in self.sources:
                source.close()

    def _ingest_all(self, start: date, end: date) -> list[SolarObservation]:
        """Ingest from all sources with graceful degradation.

        If one source fails, the pipeline continues with the remaining sources.
        This ensures partial data is still processed and available.
        """
        all_observations: list[SolarObservation] = []

        for source in self.sources:
            try:
                observations = source.fetch(start, end)
                all_observations.extend(observations)
                logger.info(
                    "source_ingested",
                    source=source.name,
                    count=len(observations),
                )
            except IngestionError as exc:
                logger.error(
                    "source_failed",
                    source=source.name,
                    error=str(exc),
                )

        return all_observations
    
    def _save_source_files(self, start: date, end: date, output_dir: str) -> None:
        """Download and save raw source data files for archival.

        Saves the same files that the legacy workflow required users to
        download manually. Useful when researchers need the raw data
        for their own analysis (e.g., plotting Figure 1a from SILSO data).

        Args:
            start: Start date of the run.
            end: End date of the run.
            output_dir: Directory to save files in.
        """
        import os
        from datetime import datetime

        sources_dir = os.path.join(output_dir, "source_files")
        os.makedirs(sources_dir, exist_ok=True)

        datestamp = datetime.now().strftime("%m%d%y")

        downloads = [
            (
                "https://www.sidc.be/SILSO/INFO/sndtotcsv.php",
                f"SN_d_tot_V2.0_{datestamp}.txt",
                "SILSO International Sunspot Number",
            ),
            (
                "https://www.spaceweather.gc.ca/solar_flux_data/daily_flux_values/fluxtable.txt",
                f"fluxtable_{datestamp}.txt",
                "Space Weather Canada 10.7cm flux",
            ),
            (
                "https://sol.spacenvironment.net/spacewx/data/mg2_atmos.dat.txt",
                f"mg2_atmos_{datestamp}.txt",
                "MgII Core-to-Wing ratio",
            ),
        ]

        # Add LASP files for each year in range
        for year in range(start.year, end.year + 1):
            yy = f"{year % 100:02d}"
            downloads.append((
                f"https://lasp.colorado.edu/eve/data_access/eve_data/lasp_soho_sem_data/long/daily_avg/{yy}_v4.day",
                f"{yy}_v4.day",
                f"LASP SEM UV {year}",
            ))

        import httpx

        for url, filename, description in downloads:
            filepath = os.path.join(sources_dir, filename)
            try:
                response = httpx.get(url, follow_redirects=True, timeout=30)
                response.raise_for_status()
                with open(filepath, "wb") as f:
                    f.write(response.content)
                logger.info("source_file_saved", file=filename, description=description)
            except Exception as exc:
                logger.warning("source_file_failed", file=filename, error=str(exc))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Solar Activity Data Pipeline",
    )
    parser.add_argument(
        "--start-date", type=date.fromisoformat,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--days-back", type=int, default=30,
        help="Number of days back from today (default: 30)",
    )
    parser.add_argument(
        "--run", type=int,
        help="HMI pipeline run number (auto-fills dates and MDI day)",
    )
    parser.add_argument(
        "--init-db", action="store_true",
        help="Initialize database tables and exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Ingest and validate but don't persist to database",
    )
    parser.add_argument(
        "--export-dat", action="store_true",
        help="Export legacy .dat file",
    )
    parser.add_argument(
        "--run-number", type=int,
        help="HMI pipeline run number (for .dat export)",
    )
    parser.add_argument(
        "--mdi-day-start", type=int,
        help="Starting MDI day number (for .dat export)",
    )
    parser.add_argument(
        "--save-sources", action="store_true",
        help="Save raw source data files alongside the .dat output",
    )
    return parser.parse_args(argv)
    

def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""
    args = parse_args(argv)
    db = Database()

    if args.init_db:
        db.create_tables()
        logger.info("database_initialized")
        return

    # Determine date range and export settings
    export_kwargs = {}

    if args.run:
        # Auto-fill everything from run registry
        run_info = get_run(args.run)
        start, end = run_info.start_date, run_info.end_date
        args.export_dat = True
        export_kwargs = {
            "run_number": args.run,
            "mdi_day_start": 0,  # not used, .dat col 1 is computed from dates
        }
        # On HPC, output directly to lab directory
        import os
        if os.path.isdir(LAB_OUTPUT_DIR):
            import src.config
            src.config.settings.dat_output_dir = LAB_OUTPUT_DIR
            logger.info("output_dir_set", path=LAB_OUTPUT_DIR)
        logger.info(
            "run_loaded",
            run=args.run,
            start=str(start),
            end=str(end),
            days=run_info.num_days,
        )
    elif args.start_date and args.end_date:
        start, end = args.start_date, args.end_date
    else:
        end = date.today()
        start = end - timedelta(days=args.days_back)

    if args.export_dat and not export_kwargs:
        if args.run_number and args.mdi_day_start:
            export_kwargs = {
                "run_number": args.run_number,
                "mdi_day_start": args.mdi_day_start,
            }
        else:
            logger.error("export_dat requires --run or (--run-number and --mdi-day-start)")
            sys.exit(1)

    pipeline = Pipeline(db=db, dry_run=args.dry_run)
    if args.save_sources:
        pipeline._save_sources = True

    pipeline.run(start, end, export_dat=args.export_dat, **export_kwargs)


if __name__ == "__main__":
    main()