"""
Processing Pipeline - Orchestrates the chain of steps
"""
from typing import List, Optional
from src.modules.processing.steps import IPipelineStep


class ProcessingPipeline:
    """
    Executes a chain of processing steps.
    If any step returns None, the pipeline stops (circuit breaker).
    """
    def __init__(self, steps: List[IPipelineStep]):
        self.steps = steps
        
    def run(self, raw_data):
        """
        Execute all steps in sequence
        
        Args:
            raw_data: Input data (typically RawTweet)
            
        Returns:
            Processed data or None if any step filtered it out
        """
        payload = raw_data
        for step in self.steps:
            payload = step.execute(payload)
            # Circuit Breaker: If any step returns None, stop.
            if payload is None:
                return None
        return payload
