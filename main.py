"""Manual entry point: same approved pipeline as run_pipeline.py."""
import sys
from run_pipeline import PipelineOrchestrator

if __name__ == '__main__':
    sys.exit(0 if PipelineOrchestrator().run() else 1)
