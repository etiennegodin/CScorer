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
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
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
    config: Dict[str, Any ] = field(default_factory=dict)
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
                raise ValueError(f"Input validation failed for step: {self.name}")
            
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
                f"Completed step: {self.name} "
                f"(duration: {result.duration:.2f}s)"
            )
            
        except RetryError as e:
            result.status = StepStatus.FAILED
            result.error = f"All retry attempts failed: {str(e)}"
            result.end_time = datetime.now()
            result.attempt_number = self.retry_attempts
            
            self.logger.error(
                f"Step failed after {self.retry_attempts} attempts: {self.name}",
                exc_info=True
            )
            
            if not self.skip_on_failure:
                raise
            
        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = str(e)
            result.end_time = datetime.now()
            
            self.logger.error(f"Step failed: {self.name}", exc_info=True)
            
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
        
        self.logger.info(f"Completed submodule: {full_name}")
        return results
    
    def get_summary(self, context: PipelineContext) -> Dict:
        """Get summary of submodule execution"""
        submodule_results = {
            name: result
            for name, result in context.results.items()
            if name.startswith(f"{self.name}.")
        }
        
        completed = sum(
            1 for r in submodule_results.values()
            if r.status == StepStatus.COMPLETED
        )
        
        return {
            'name': self.name,
            'total_steps': len(self.steps),
            'completed': completed,
            'status': 'completed' if completed == len(self.steps) else 'partial'
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
            
            self.logger.info(f"Completed module: {self.name}")
            
        except Exception as e:
            self.logger.error(f"Module failed: {self.name}", exc_info=True)
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
        
        completed = sum(
            1 for r in module_results.values()
            if r.status == StepStatus.COMPLETED
        )
        failed = sum(
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
            'completed': completed,
            'failed': failed,
            'total_duration_seconds': round(total_duration, 2),
            'status': 'completed' if completed == len(module_results) else 'partial'
        }

class Pipeline:
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
            'completed_steps': [
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
        config: Dict,
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
            config = config,
            data=initial_data or {},
            checkpoint_dir=self.checkpoint_dir
        )
        
        # Handle checkpoint resume
        completed_steps = set()
        if resume_from_checkpoint and self.checkpoint_dir:
            checkpoint = self._load_checkpoint()
            if checkpoint:
                completed_steps = set(checkpoint.get('completed_steps', []))
                self.logger.info(
                    f"Resuming from checkpoint with {len(completed_steps)} "
                    f"completed steps"
                )
                
        # Determine which modules to run
        if only_modules:
            modules_to_run = only_modules
            
        else:
            start_idx = 0
            end_idx = len(self.module_order)
            
            if from_module:
                start_idx = self.module_order.index(from_module)
            if to_module:
                end_idx = self.module_order.index(to_module) + 1
            
            modules_to_run = self.module_order[start_idx:end_idx]
            
        # Remove skipped modules
        if skip_modules:
            modules_to_run = [m for m in modules_to_run if m not in skip_modules]
            
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Running pipeline: {self.name}")
        self.logger.info(f"Modules to execute: {modules_to_run}")
        self.logger.info(f"{'='*60}\n")
        
        # Execute modules
        for module_name in modules_to_run:
            # Check if entire module is completed (all its steps)
            module = self.modules[module_name]
            
            if resume_from_checkpoint:
                # Count how many steps in this module are already completed
                module_steps_completed = sum(
                    1 for step_name in completed_steps
                    if step_name.startswith(f"{module_name}.")
                )
                
                # If all steps are completed, skip the module
                total_module_steps = self._count_module_steps(module)
                if module_steps_completed == total_module_steps:
                    self.logger.info(f"Skipping completed module: {module_name}")
                    continue
            try:
                module.run(context)
                self._save_checkpoint(context)
            except Exception as e:
                self.logger.error(
                    f"Pipeline failed at module: {module_name}",
                    exc_info=True
                )
                self._save_checkpoint(context)
                raise
            
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Pipeline completed: {self.name}")
        self.logger.info(f"{'='*60}\n")
        
        return context
    
    def _count_module_steps(self, module: Module) -> int:
        """Count total steps in a module including submodules"""
        count = 0
        for component in module.components:
            if isinstance(component, SubModule):
                count += len(component.steps)
            else:
                count += 1
        return count
    
    def get_status(self, context: PipelineContext) -> Dict[str, Any]:
        """Get detailed pipeline status with module-level summaries"""
        module_summaries = {}
        for module_name in self.module_order:
            module = self.modules[module_name]
            module_summaries[module_name] = module.get_summary(context)
        
        total_steps = sum(m['total_steps'] for m in module_summaries.values())
        completed_steps = sum(m['completed'] for m in module_summaries.values())
        
        return {
            'pipeline_name': self.name,
            'total_modules': len(self.modules),
            'total_steps': total_steps,
            'completed_steps': completed_steps,
            'progress': f"{completed_steps}/{total_steps}",
            'modules': module_summaries
        }
