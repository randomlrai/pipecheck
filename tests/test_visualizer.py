"""Tests for DAG visualization module."""

import json
import unittest
from pipecheck.dag import DAG, Task
from pipecheck.visualizer import DAGVisualizer


class TestDAGVisualizer(unittest.TestCase):
    """Test cases for DAGVisualizer class."""

    def setUp(self):
        """Set up test DAG for visualization tests."""
        self.dag = DAG()
        self.dag.add_task(Task("extract", metadata={"description": "Extract data", "timeout": 300}))
        self.dag.add_task(Task("transform", dependencies=["extract"], metadata={"timeout": 600}))
        self.dag.add_task(Task("load", dependencies=["transform"], metadata={"timeout": 400}))
        self.visualizer = DAGVisualizer(self.dag)

    def test_visualizer_creation(self):
        """Test that visualizer is created with a DAG."""
        self.assertIsNotNone(self.visualizer)
        self.assertEqual(self.visualizer.dag, self.dag)

    def test_to_mermaid(self):
        """Test Mermaid diagram generation."""
        mermaid = self.visualizer.to_mermaid()
        self.assertIn("graph TD", mermaid)
        self.assertIn("extract", mermaid)
        self.assertIn("transform", mermaid)
        self.assertIn("load", mermaid)
        self.assertIn("extract --> transform", mermaid)
        self.assertIn("transform --> load", mermaid)

    def test_to_mermaid_with_descriptions(self):
        """Test Mermaid diagram includes descriptions."""
        mermaid = self.visualizer.to_mermaid()
        self.assertIn("Extract data", mermaid)

    def test_to_dot(self):
        """Test DOT format generation."""
        dot = self.visualizer.to_dot()
        self.assertIn("digraph DAG", dot)
        self.assertIn('"extract"', dot)
        self.assertIn('"transform"', dot)
        self.assertIn('"load"', dot)
        self.assertIn('"extract" -> "transform"', dot)
        self.assertIn('"transform" -> "load"', dot)

    def test_to_dot_includes_timeout(self):
        """Test DOT format includes timeout metadata."""
        dot = self.visualizer.to_dot()
        self.assertIn("Timeout: 300s", dot)
        self.assertIn("Timeout: 600s", dot)

    def test_to_json(self):
        """Test JSON representation generation."""
        json_str = self.visualizer.to_json()
        data = json.loads(json_str)
        
        self.assertIn("tasks", data)
        self.assertEqual(len(data["tasks"]), 3)
        
        task_ids = [task["id"] for task in data["tasks"]]
        self.assertIn("extract", task_ids)
        self.assertIn("transform", task_ids)
        self.assertIn("load", task_ids)

    def test_get_task_levels(self):
        """Test task level calculation."""
        levels = self.visualizer.get_task_levels()
        
        self.assertEqual(levels["extract"], 0)
        self.assertEqual(levels["transform"], 1)
        self.assertEqual(levels["load"], 2)

    def test_get_task_levels_parallel_tasks(self):
        """Test task levels with parallel branches."""
        dag = DAG()
        dag.add_task(Task("start"))
        dag.add_task(Task("branch1", dependencies=["start"]))
        dag.add_task(Task("branch2", dependencies=["start"]))
        dag.add_task(Task("merge", dependencies=["branch1", "branch2"]))
        
        visualizer = DAGVisualizer(dag)
        levels = visualizer.get_task_levels()
        
        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["branch1"], 1)
        self.assertEqual(levels["branch2"], 1)
        self.assertEqual(levels["merge"], 2)


if __name__ == "__main__":
    unittest.main()
