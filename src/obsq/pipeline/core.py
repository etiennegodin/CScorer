import logging
import json
import asyncio
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from abc import ABC, abstractmethod

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    RetryError
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class StepStatus(str, Enum):
    """Status of apipeline step """
    
    init = "init"    
    requested = "requested" 
    PENDING  = 'pending'
    ready = "ready"
    LOCAL = 'LOCAL'
    incomplete = 'incomplete'
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "FAILED"
    SKIPPED = "skipped"
    
@dataclass
class StepResult:
    """Result of a step execution"""
    step_name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    attempt_number: int = 1
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def to_dict(self) -> Dict:
        """
        :return: Returns a step's results as dict
        :rtype: Dict
        """
        data = asdict(self)
        data['status'] = self.status.value
        data['start_time'] = self.start_time.isoformat() if self.start_time else None
        data['end_time'] = self.end_time.isoformat() if self.end_time else None
        if self.output is not None:
            data['output'] = f"<{type(self.output).__name__} object>"
        return data
    
@dataclass
class PipelineContext:
    """Shared context passed between pipeline steps"""
    data: Dict[str, Any] = field(default_factory=dict)
    results: Dict[str, StepResult] = field(default_factory=dict)
    checkpoint_dir: Optional[Path] = None
    
    def get(self, key: str, default=None) -> Any:
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any):
        self.data[key] = value
    
    def get_step_output(self, step_name: str) -> Any:
        if step_name in self.results:
            return self.results[step_name].output
        return None
    
    def get_module_outputs(self, module_name: str) -> Dict[str, Any]:
        """Get all outputs from steps in a module"""
        return {
            name: result.output
            for name, result in self.results.items()
            if name.startswith(f"{module_name}.")
        }

class Step(ABC):
    """Base class for pipeline steps"""
    def __init__(
        self,
        name: str,
        retry_attempts: int = 3,
        retry_wait_multiplier: int = 1,
        retry_wait_max: int = 10,
        skip_on_failure: bool = False
    ):
        self.name = name
        self.retry_attempts = retry_attempts
        self.retry_wait_multiplier = retry_wait_multiplier
        self.retry_wait_max = retry_wait_max
        self.skip_on_failure = skip_on_failure
        self.logger = logging.getLogger(f"{__name__}.{self.name}")
        
    @abstractmethod
    def execute(self, context: PipelineContext) -> Any:
        pass
    
    def validate_inputs(self, context: PipelineContext) -> bool:
        return True
    
    def run(self, context: PipelineContext) -> StepResult:
        
        result = StepResult(
            step_name=self.name,
            status=StepStatus.RUNNING,
            start_time=datetime.now()
        )
        
        try:
            if not self.validate_inputs(context):
                raise ValueError(f"Input validation FAILED for step: {self.name}")
            
            self.logger.info(f"Starting step: {self.name}")
            
            @retry(
                stop=stop_after_attempt(self.retry_attempts),
                wait=wait_exponential(
                    multiplier=self.retry_wait_multiplier,
                    max=self.retry_wait_max
                ),
                reraise=True
            )   
            def _execute_with_retry():
                return self.execute(context)        
            
            output = _execute_with_retry()
            result.output = output
            result.status = StepStatus.COMPLETED
            result.end_time = datetime.now()
            
            self.logger.info(
                f"COMPLETED step: {self.name} "
                f"(duration: {result.duration:.2f}s)"
            )
            
        except RetryError as e:
            result.status = StepStatus.FAILED
            result.error = f"All retry attempts FAILED: {str(e)}"
            result.end_time = datetime.now()
            result.attempt_number = self.retry_attempts
            
            self.logger.error(
                f"Step FAILED after {self.retry_attempts} attempts: {self.name}",
                exc_info=True
            )
            
            if not self.skip_on_failure:
                raise
            
        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = str(e)
            result.end_time = datetime.now()
            
            self.logger.error(f"Step FAILED: {self.name}", exc_info=True)
            
            if not self.skip_on_failure:
                raise
            
        context.results[self.name] = result
        return result
    
class SubModule:
    """
    Container for related steps that form a logical sub-unit.
    
    Example: Within a "feature_engineering" module, you might have
    "time_features" and "categorical_features" as submodules.
    """
    
    def __init__(self, name: str, steps: List[Step]):
        self.name = name
        self.steps = {step.name: step for step in steps}
        self.step_order = [step.name for step in steps]
        self.logger = logging.getLogger(f"{__name__}.SubModule.{name}")
    
    def run(
        self,
        context: PipelineContext,
        parent_name: str = ""
    ) -> Dict[str, StepResult]:
        """Run all steps in this submodule"""
        full_name = f"{parent_name}.{self.name}" if parent_name else self.name
        self.logger.info(f"Starting submodule: {full_name}")
        
        results = {}
        for step_name in self.step_order:
            step = self.steps[step_name]
            # Prefix step name with submodule hierarchy
            step.name = f"{full_name}.{step_name}"
            result = step.run(context)
            results[step.name] = result
        
        self.logger.info(f"COMPLETED submodule: {full_name}")
        return results
    
    def get_summary(self, context: PipelineContext) -> Dict:
        """Get summary of submodule execution"""
        submodule_results = {
            name: result
            for name, result in context.results.items()
            if name.startswith(f"{self.name}.")
        }
        
        COMPLETED = sum(
            1 for r in submodule_results.values()
            if r.status == StepStatus.COMPLETED
        )
        
        return {
            'name': self.name,
            'total_steps': len(self.steps),
            'COMPLETED': COMPLETED,
            'status': 'COMPLETED' if COMPLETED == len(self.steps) else 'partial'
        }
        
class Module:
    """
    High-level container that groups related submodules and steps.
    
    Example modules: "data_loading", "feature_engineering", "model_training"
    Each module can contain multiple submodules and/or individual steps.
    """
    
    def __init__(
        self,
        name: str,
        components: List[Union[Step, SubModule]],
        skip_on_module_failure: bool = False
    ):
        self.name = name
        self.components = components
        self.skip_on_module_failure = skip_on_module_failure
        self.logger = logging.getLogger(f"{__name__}.Module.{name}")
    
    def run(self, context: PipelineContext) -> Dict[str, StepResult]:
        """Run all components in this module"""
        self.logger.info(f"=" * 60)
        self.logger.info(f"Starting module: {self.name}")
        self.logger.info(f"=" * 60)
        
        results = {}
        
        try:
            for component in self.components:
                if isinstance(component, SubModule):
                    submodule_results = component.run(context, parent_name=self.name)
                    results.update(submodule_results)
                else:  # It's a Step
                    component.name = f"{self.name}.{component.name}"
                    result = component.run(context)
                    results[component.name] = result
            
            self.logger.info(f"COMPLETED module: {self.name}")
            
        except Exception as e:
            self.logger.error(f"Module FAILED: {self.name}", exc_info=True)
            if not self.skip_on_module_failure:
                raise
        
        return results
    
    def get_summary(self, context: PipelineContext) -> Dict:
        """Get summary of module execution"""
        module_results = {
            name: result
            for name, result in context.results.items()
            if name.startswith(f"{self.name}.")
        }
        
        COMPLETED = sum(
            1 for r in module_results.values()
            if r.status == StepStatus.COMPLETED
        )
        FAILED = sum(
            1 for r in module_results.values()
            if r.status == StepStatus.FAILED
        )
        
        total_duration = sum(
            r.duration for r in module_results.values()
            if r.duration is not None
        )
        
        return {
            'name': self.name,
            'total_steps': len(module_results),
            'COMPLETED': COMPLETED,
            'FAILED': FAILED,
            'total_duration_seconds': round(total_duration, 2),
            'status': 'COMPLETED' if COMPLETED == len(module_results) else 'partial'
        }

class ModularPipeline:
    """
    Pipeline that organizes steps into modules and submodules.
    
    This makes it easy to:
    - Run specific modules during development
    - Skip modules you're not working on
    - Organize complex pipelines logically
    """
    
    def __init__(
        self,
        name: str,
        modules: List[Module],
        checkpoint_dir: Optional[Union[str, Path]] = None
    ):
        self.name = name
        self.modules = {module.name: module for module in modules}
        self.module_order = [module.name for module in modules]
        self.checkpoint_dir = Path(checkpoint_dir) if checkpoint_dir else None
        self.logger = logging.getLogger(f"{__name__}.Pipeline.{name}")
        
        if self.checkpoint_dir:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
    def _get_checkpoint_path(self) -> Path:
        return self.checkpoint_dir / f"{self.name}_checkpoint.json"
    
    def _save_checkpoint(self, context: PipelineContext):
        if not self.checkpoint_dir:
            return
        
        checkpoint = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_name': self.name,
            'COMPLETED_steps': [
                name for name, result in context.results.items()
                if result.status == StepStatus.COMPLETED
            ],
            'results': {
                name: result.to_dict()
                for name, result in context.results.items()
            }
        }
        
        checkpoint_path = self._get_checkpoint_path()
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        
        self.logger.info(f"Checkpoint saved: {checkpoint_path}")
        
    def _load_checkpoint(self) -> Optional[Dict]:
        if not self.checkpoint_dir:
            return None
        
        checkpoint_path = self._get_checkpoint_path()
        if not checkpoint_path.exists():
            return None
        
        with open(checkpoint_path, 'r') as f:
            checkpoint = json.load(f)
        
        self.logger.info(f"Checkpoint loaded: {checkpoint_path}")
        return checkpoint
    
    def run(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        from_module: Optional[str] = None,
        to_module: Optional[str] = None,
        only_modules: Optional[List[str]] = None,
        skip_modules: Optional[List[str]] = None,
        resume_from_checkpoint: bool = False
    ) -> PipelineContext:
        """
        Run the pipeline with flexible module selection.
        
        Args:
            initial_data: Initial data to populate context
            from_module: Start from this module (inclusive)
            to_module: Stop at this module (inclusive)
            only_modules: Run only these modules (list of module names)
            skip_modules: Skip these modules (list of module names)
            resume_from_checkpoint: Resume from last checkpoint
            
        Returns:
            Pipeline context with results
        """
        # Initialize context
        context = PipelineContext(
            data=initial_data or {},
            checkpoint_dir=self.checkpoint_dir
        )
        