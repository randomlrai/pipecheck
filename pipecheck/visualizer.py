"""Visualization module for DAG structures."""

import json
from typing import Dict, List, Set, Tuple
from .dag import DAG, Task


class DAGVisualizer:
    """Visualizes DAG structures in various formats."""

    def __init__(self, dag: DAG):
        """Initialize visualizer with a DAG.
        
        Args:
            dag: The DAG to visualize
        """
        self.dag = dag

    def to_mermaid(self) -> str:
        """Generate Mermaid diagram syntax for the DAG.
        
        Returns:
            String containing Mermaid diagram definition
        """
        lines = ["graph TD"]
        
        # Add nodes with labels
        for task_id, task in self.dag.tasks.items():
            label = task_id
            if task.metadata.get('description'):
                label = f"{task_id}: {task.metadata['description']}"
            lines.append(f"    {task_id}[{label}]")
        
        # Add edges
        for task_id, task in self.dag.tasks.items():
            for dep in task.dependencies:
                lines.append(f"    {dep} --> {task_id}")
        
        return "\n".join(lines)

    def to_dot(self) -> str:
        """Generate Graphviz DOT format for the DAG.
        
        Returns:
            String containing DOT graph definition
        """
        lines = ["digraph DAG {"]
        lines.append("    rankdir=LR;")
        lines.append("    node [shape=box, style=rounded];")
        
        # Add nodes
        for task_id, task in self.dag.tasks.items():
            timeout = task.metadata.get('timeout', 'N/A')
            label = f"{task_id}\\nTimeout: {timeout}s"
            lines.append(f'    "{task_id}" [label="{label}"];')
        
        # Add edges
        for task_id, task in self.dag.tasks.items():
            for dep in task.dependencies:
                lines.append(f'    "{dep}" -> "{task_id}";')
        
        lines.append("}")
        return "\n".join(lines)

    def to_json(self) -> str:
        """Generate JSON representation of the DAG.
        
        Returns:
            JSON string representing the DAG structure
        """
        dag_dict = {
            "tasks": [
                {
                    "id": task_id,
                    "dependencies": list(task.dependencies),
                    "metadata": task.metadata
                }
                for task_id, task in self.dag.tasks.items()
            ]
        }
        return json.dumps(dag_dict, indent=2)

    def get_task_levels(self) -> Dict[str, int]:
        """Calculate the level/depth of each task in the DAG.
        
        Returns:
            Dictionary mapping task IDs to their depth level
        """
        levels = {}
        
        def calculate_level(task_id: str) -> int:
            if task_id in levels:
                return levels[task_id]
            
            task = self.dag.tasks[task_id]
            if not task.dependencies:
                levels[task_id] = 0
            else:
                max_dep_level = max(calculate_level(dep) for dep in task.dependencies)
                levels[task_id] = max_dep_level + 1
            
            return levels[task_id]
        
        for task_id in self.dag.tasks:
            calculate_level(task_id)
        
        return levels
