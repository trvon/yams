#!/usr/bin/env python3
"""
Test result visualization script for YAMS.
Generates charts and reports from test and benchmark data.
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import datetime

# Try to import plotting libraries (optional)
try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not installed. Charts will not be generated.")
    print("Install with: pip install matplotlib")

class TestResultVisualizer:
    def __init__(self, results_path: Path):
        """Initialize visualizer with test results."""
        self.results_path = results_path
        self.data = self.load_results()
        
    def load_results(self) -> Dict:
        """Load test results from JSON file."""
        with open(self.results_path, 'r') as f:
            return json.load(f)
    
    def generate_html_report(self, output_path: Path):
        """Generate interactive HTML dashboard."""
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>YAMS Test Results Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .metric {
            display: inline-block;
            margin: 10px 20px;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #eee;
        }
        .pass { color: #22c55e; }
        .fail { color: #ef4444; }
        .regression { background: #fee2e2; }
        .improvement { background: #dcfce7; }
        .chart-container {
            width: 100%;
            height: 400px;
            margin: 20px 0;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }
        .badge-success { background: #22c55e; color: white; }
        .badge-danger { background: #ef4444; color: white; }
        .badge-warning { background: #f59e0b; color: white; }
        .badge-info { background: #3b82f6; color: white; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 YAMS Test Results Dashboard</h1>
            <p>Generated: {timestamp}</p>
        </div>
        
        {summary_section}
        {benchmark_section}
        {coverage_section}
        {test_details_section}
    </div>
    
    <script>
        {chart_scripts}
    </script>
</body>
</html>
"""
        
        # Generate sections
        summary = self._generate_summary_section()
        benchmarks = self._generate_benchmark_section()
        coverage = self._generate_coverage_section()
        details = self._generate_test_details_section()
        scripts = self._generate_chart_scripts()
        
        # Fill template
        html = html.format(
            timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            summary_section=summary,
            benchmark_section=benchmarks,
            coverage_section=coverage,
            test_details_section=details,
            chart_scripts=scripts
        )
        
        # Write HTML file
        with open(output_path, 'w') as f:
            f.write(html)
        
        print(f"📊 HTML dashboard generated: {output_path}")
    
    def _generate_summary_section(self) -> str:
        """Generate summary metrics section."""
        total_tests = self.data.get('total_tests', 0)
        passed = self.data.get('passed', 0)
        failed = self.data.get('failed', 0)
        skipped = self.data.get('skipped', 0)
        duration = self.data.get('duration_seconds', 0)
        
        pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        
        return f"""
        <div class="card">
            <h2>Test Summary</h2>
            <div class="metrics">
                <div class="metric">
                    <div class="metric-value">{total_tests}</div>
                    <div class="metric-label">Total Tests</div>
                </div>
                <div class="metric">
                    <div class="metric-value" style="color: #22c55e">{passed}</div>
                    <div class="metric-label">Passed</div>
                </div>
                <div class="metric">
                    <div class="metric-value" style="color: #ef4444">{failed}</div>
                    <div class="metric-label">Failed</div>
                </div>
                <div class="metric">
                    <div class="metric-value" style="color: #f59e0b">{skipped}</div>
                    <div class="metric-label">Skipped</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{pass_rate:.1f}%</div>
                    <div class="metric-label">Pass Rate</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{duration:.2f}s</div>
                    <div class="metric-label">Duration</div>
                </div>
            </div>
            <canvas id="testResultChart" style="max-width: 300px; margin: 20px auto;"></canvas>
        </div>
        """
    
    def _generate_benchmark_section(self) -> str:
        """Generate benchmark results section."""
        if 'benchmarks' not in self.data and 'summary' not in self.data:
            return ""
        
        rows = []
        benchmarks = self.data.get('summary', {})
        
        for name, details in benchmarks.items():
            trend = details.get('trend', 'stable')
            trend_icon = "↗️" if trend == "degrading" else "↘️" if trend == "improving" else "→"
            trend_class = "regression" if trend == "degrading" else "improvement" if trend == "improving" else ""
            
            rows.append(f"""
                <tr class="{trend_class}">
                    <td>{name}</td>
                    <td>{details.get('latest', 0):.2f} {details.get('unit', '')}</td>
                    <td>{details.get('average', 0):.2f}</td>
                    <td>{details.get('min', 0):.2f}</td>
                    <td>{details.get('max', 0):.2f}</td>
                    <td>{trend_icon} {trend}</td>
                    <td>{details.get('runs', 0)}</td>
                </tr>
            """)
        
        return f"""
        <div class="card">
            <h2>Benchmark Results</h2>
            <table>
                <thead>
                    <tr>
                        <th>Benchmark</th>
                        <th>Latest</th>
                        <th>Average</th>
                        <th>Min</th>
                        <th>Max</th>
                        <th>Trend</th>
                        <th>Runs</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
            <canvas id="benchmarkChart" style="max-height: 400px; margin-top: 20px;"></canvas>
        </div>
        """
    
    def _generate_coverage_section(self) -> str:
        """Generate code coverage section."""
        coverage = self.data.get('coverage', {})
        if not coverage:
            return ""
        
        line_coverage = coverage.get('line_percent', 0)
        branch_coverage = coverage.get('branch_percent', 0)
        function_coverage = coverage.get('function_percent', 0)
        
        return f"""
        <div class="card">
            <h2>Code Coverage</h2>
            <div class="metrics">
                <div class="metric">
                    <div class="metric-value">{line_coverage:.1f}%</div>
                    <div class="metric-label">Line Coverage</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{branch_coverage:.1f}%</div>
                    <div class="metric-label">Branch Coverage</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{function_coverage:.1f}%</div>
                    <div class="metric-label">Function Coverage</div>
                </div>
            </div>
            <canvas id="coverageChart" style="max-width: 500px; margin: 20px auto;"></canvas>
        </div>
        """
    
    def _generate_test_details_section(self) -> str:
        """Generate detailed test results section."""
        if 'test_cases' not in self.data:
            return ""
        
        rows = []
        for test in self.data.get('test_cases', []):
            status_badge = '<span class="badge badge-success">PASS</span>' if test['passed'] else \
                          '<span class="badge badge-danger">FAIL</span>'
            duration = test.get('duration_ms', 0)
            
            rows.append(f"""
                <tr>
                    <td>{test['name']}</td>
                    <td>{test.get('suite', 'Unknown')}</td>
                    <td>{status_badge}</td>
                    <td>{duration:.2f}ms</td>
                    <td>{test.get('error', '')}</td>
                </tr>
            """)
        
        return f"""
        <div class="card">
            <h2>Test Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Suite</th>
                        <th>Status</th>
                        <th>Duration</th>
                        <th>Error</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
        """
    
    def _generate_chart_scripts(self) -> str:
        """Generate Chart.js scripts for visualization."""
        return """
        // Test Results Pie Chart
        var testCtx = document.getElementById('testResultChart');
        if (testCtx) {
            new Chart(testCtx, {
                type: 'doughnut',
                data: {
                    labels: ['Passed', 'Failed', 'Skipped'],
                    datasets: [{
                        data: [""" + str(self.data.get('passed', 0)) + """, 
                               """ + str(self.data.get('failed', 0)) + """, 
                               """ + str(self.data.get('skipped', 0)) + """],
                        backgroundColor: ['#22c55e', '#ef4444', '#f59e0b']
                    }]
                }
            });
        }
        
        // Benchmark Trend Chart
        var benchCtx = document.getElementById('benchmarkChart');
        if (benchCtx) {
            // This would need historical data to show trends
            // Placeholder for now
        }
        
        // Coverage Bar Chart
        var coverageCtx = document.getElementById('coverageChart');
        if (coverageCtx) {
            new Chart(coverageCtx, {
                type: 'bar',
                data: {
                    labels: ['Line', 'Branch', 'Function'],
                    datasets: [{
                        label: 'Coverage %',
                        data: [""" + str(self.data.get('coverage', {}).get('line_percent', 0)) + """,
                               """ + str(self.data.get('coverage', {}).get('branch_percent', 0)) + """,
                               """ + str(self.data.get('coverage', {}).get('function_percent', 0)) + """],
                        backgroundColor: '#667eea'
                    }]
                },
                options: {
                    scales: {
                        y: {
                            beginAtZero: true,
                            max: 100
                        }
                    }
                }
            });
        }
        """
    
    def plot_benchmark_trends(self, output_dir: Path):
        """Generate matplotlib charts for benchmark trends."""
        if not HAS_MATPLOTLIB:
            return
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Plot benchmark comparisons
        if 'summary' in self.data:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            fig.suptitle('YAMS Benchmark Analysis', fontsize=16)
            
            benchmarks = self.data['summary']
            names = list(benchmarks.keys())[:4]  # Top 4 benchmarks
            
            for idx, (ax, name) in enumerate(zip(axes.flat, names)):
                if name not in benchmarks:
                    ax.axis('off')
                    continue
                
                details = benchmarks[name]
                values = [
                    details.get('min', 0),
                    details.get('average', 0),
                    details.get('max', 0),
                    details.get('latest', 0)
                ]
                labels = ['Min', 'Avg', 'Max', 'Latest']
                colors = ['green', 'blue', 'orange', 'red']
                
                ax.bar(labels, values, color=colors, alpha=0.7)
                ax.set_title(name)
                ax.set_ylabel(details.get('unit', 'Value'))
                ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(output_dir / 'benchmark_analysis.png', dpi=150)
            plt.close()
            
            print(f"📈 Benchmark charts saved to {output_dir}")
    
    def generate_markdown_summary(self, output_path: Path):
        """Generate a markdown summary of test results."""
        md = f"""# YAMS Test Results Summary

Generated: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Tests | {self.data.get('total_tests', 0)} |
| Passed | {self.data.get('passed', 0)} |
| Failed | {self.data.get('failed', 0)} |
| Skipped | {self.data.get('skipped', 0)} |
| Pass Rate | {(self.data.get('passed', 0) / max(self.data.get('total_tests', 1), 1) * 100):.1f}% |
| Duration | {self.data.get('duration_seconds', 0):.2f}s |

"""
        
        # Add benchmark summary if available
        if 'summary' in self.data:
            md += "## Benchmark Summary\n\n"
            md += "| Benchmark | Latest | Average | Trend |\n"
            md += "|-----------|--------|---------|-------|\n"
            
            for name, details in self.data['summary'].items():
                trend = details.get('trend', 'stable')
                trend_icon = "📈" if trend == "degrading" else "📉" if trend == "improving" else "➡️"
                md += f"| {name} | {details.get('latest', 0):.2f} {details.get('unit', '')} | "
                md += f"{details.get('average', 0):.2f} | {trend_icon} {trend} |\n"
        
        # Add coverage if available
        if 'coverage' in self.data:
            coverage = self.data['coverage']
            md += f"""
## Code Coverage

- Line Coverage: {coverage.get('line_percent', 0):.1f}%
- Branch Coverage: {coverage.get('branch_percent', 0):.1f}%
- Function Coverage: {coverage.get('function_percent', 0):.1f}%
"""
        
        # Write markdown file
        with open(output_path, 'w') as f:
            f.write(md)
        
        print(f"📝 Markdown summary saved to {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Visualize YAMS test results')
    parser.add_argument('results', type=Path, help='Test results JSON file')
    parser.add_argument('--output-dir', type=Path, default=Path('test_reports'),
                       help='Output directory for reports')
    parser.add_argument('--format', choices=['html', 'markdown', 'charts', 'all'],
                       default='all', help='Output format')
    
    args = parser.parse_args()
    
    # Validate input
    if not args.results.exists():
        print(f"Error: Results file not found: {args.results}")
        sys.exit(1)
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate reports
    visualizer = TestResultVisualizer(args.results)
    
    if args.format in ['html', 'all']:
        visualizer.generate_html_report(args.output_dir / 'dashboard.html')
    
    if args.format in ['markdown', 'all']:
        visualizer.generate_markdown_summary(args.output_dir / 'summary.md')
    
    if args.format in ['charts', 'all']:
        visualizer.plot_benchmark_trends(args.output_dir / 'charts')
    
    print(f"\n✅ Reports generated in {args.output_dir}")

if __name__ == '__main__':
    main()