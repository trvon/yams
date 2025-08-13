#!/usr/bin/env python3
"""
Benchmark regression detection script for YAMS.
Compares current benchmark results against baseline and detects regressions.
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

class RegressionChecker:
    def __init__(self, threshold: float = 0.1):
        """
        Initialize regression checker.
        
        Args:
            threshold: Percentage change to consider as regression (default 10%)
        """
        self.threshold = threshold
        self.regressions = []
        self.improvements = []
        
    def load_results(self, path: Path) -> Dict:
        """Load benchmark results from JSON file."""
        with open(path, 'r') as f:
            return json.load(f)
    
    def compare_benchmarks(self, current: Dict, baseline: Dict) -> Tuple[bool, List[Dict]]:
        """
        Compare current results with baseline.
        
        Returns:
            Tuple of (has_regression, comparison_details)
        """
        comparisons = []
        has_regression = False
        
        # Extract benchmark data
        current_benchmarks = self._extract_benchmarks(current)
        baseline_benchmarks = self._extract_benchmarks(baseline)
        
        for name, current_value in current_benchmarks.items():
            if name not in baseline_benchmarks:
                print(f"⚠️  New benchmark: {name} (no baseline)")
                continue
            
            baseline_value = baseline_benchmarks[name]
            
            # Calculate percentage change
            if baseline_value['value'] == 0:
                continue
                
            change = ((current_value['value'] - baseline_value['value']) / 
                     baseline_value['value']) * 100
            
            # Determine if regression based on unit type
            is_time_based = any(unit in current_value.get('unit', '') 
                               for unit in ['ms', 'us', 'ns', 'sec'])
            
            if is_time_based:
                # For time: increase is bad
                is_regression = change > self.threshold * 100
                is_improvement = change < -self.threshold * 100
            else:
                # For throughput: decrease is bad
                is_regression = change < -self.threshold * 100
                is_improvement = change > self.threshold * 100
            
            comparison = {
                'name': name,
                'baseline': baseline_value['value'],
                'current': current_value['value'],
                'unit': current_value.get('unit', ''),
                'change_percent': change,
                'is_regression': is_regression,
                'is_improvement': is_improvement
            }
            
            comparisons.append(comparison)
            
            if is_regression:
                has_regression = True
                self.regressions.append(comparison)
            elif is_improvement:
                self.improvements.append(comparison)
        
        return has_regression, comparisons
    
    def _extract_benchmarks(self, data: Dict) -> Dict:
        """Extract benchmark values from various JSON formats."""
        benchmarks = {}
        
        # Try different JSON structures
        if 'benchmarks' in data:
            # Google Benchmark format
            for bench in data['benchmarks']:
                name = bench['name']
                benchmarks[name] = {
                    'value': bench.get('real_time', bench.get('cpu_time', 0)),
                    'unit': bench.get('time_unit', 'ns')
                }
        elif 'results' in data:
            # Custom format
            for result in data['results']:
                benchmarks[result['name']] = {
                    'value': result['value'],
                    'unit': result.get('unit', '')
                }
        elif 'summary' in data:
            # Summary format
            for name, details in data['summary'].items():
                benchmarks[name] = {
                    'value': details.get('latest', details.get('average', 0)),
                    'unit': details.get('unit', '')
                }
        
        return benchmarks
    
    def print_report(self, comparisons: List[Dict]):
        """Print comparison report."""
        print("\n" + "="*80)
        print("BENCHMARK COMPARISON REPORT")
        print("="*80)
        
        if self.regressions:
            print("\n❌ REGRESSIONS DETECTED:")
            print("-"*40)
            for reg in self.regressions:
                print(f"  {reg['name']}:")
                print(f"    Baseline: {reg['baseline']:.2f} {reg['unit']}")
                print(f"    Current:  {reg['current']:.2f} {reg['unit']}")
                print(f"    Change:   {reg['change_percent']:+.1f}%")
        
        if self.improvements:
            print("\n✅ IMPROVEMENTS:")
            print("-"*40)
            for imp in self.improvements:
                print(f"  {imp['name']}:")
                print(f"    Baseline: {imp['baseline']:.2f} {imp['unit']}")
                print(f"    Current:  {imp['current']:.2f} {imp['unit']}")
                print(f"    Change:   {imp['change_percent']:+.1f}%")
        
        # Print summary table
        print("\n" + "="*80)
        print("FULL COMPARISON:")
        print("-"*80)
        print(f"{'Benchmark':<40} {'Baseline':>12} {'Current':>12} {'Change':>10} {'Status':>10}")
        print("-"*80)
        
        for comp in sorted(comparisons, key=lambda x: x['change_percent'], reverse=True):
            status = "❌ REGRESS" if comp['is_regression'] else \
                    "✅ IMPROVE" if comp['is_improvement'] else \
                    "→ STABLE"
            
            print(f"{comp['name']:<40} "
                  f"{comp['baseline']:>12.2f} "
                  f"{comp['current']:>12.2f} "
                  f"{comp['change_percent']:>+9.1f}% "
                  f"{status:>10}")
        
        print("="*80)
    
    def generate_json_report(self, comparisons: List[Dict], output_path: Path):
        """Generate JSON report for CI integration."""
        report = {
            'has_regression': len(self.regressions) > 0,
            'threshold_percent': self.threshold * 100,
            'summary': {
                'total_benchmarks': len(comparisons),
                'regressions': len(self.regressions),
                'improvements': len(self.improvements),
                'stable': len(comparisons) - len(self.regressions) - len(self.improvements)
            },
            'regressions': self.regressions,
            'improvements': self.improvements,
            'all_comparisons': comparisons
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📊 JSON report saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Check for benchmark regressions')
    parser.add_argument('current', type=Path, help='Current benchmark results (JSON)')
    parser.add_argument('baseline', type=Path, help='Baseline benchmark results (JSON)')
    parser.add_argument('--threshold', type=float, default=0.1,
                       help='Regression threshold as decimal (default: 0.1 = 10%%)')
    parser.add_argument('--output', type=Path, default=None,
                       help='Output JSON report path')
    parser.add_argument('--fail-on-regression', action='store_true',
                       help='Exit with non-zero code if regression detected')
    
    args = parser.parse_args()
    
    # Validate input files
    if not args.current.exists():
        print(f"Error: Current results file not found: {args.current}")
        sys.exit(1)
    
    if not args.baseline.exists():
        print(f"Error: Baseline file not found: {args.baseline}")
        sys.exit(1)
    
    # Run regression check
    checker = RegressionChecker(threshold=args.threshold)
    
    try:
        current_data = checker.load_results(args.current)
        baseline_data = checker.load_results(args.baseline)
        
        has_regression, comparisons = checker.compare_benchmarks(current_data, baseline_data)
        
        # Print report
        checker.print_report(comparisons)
        
        # Generate JSON report if requested
        if args.output:
            checker.generate_json_report(comparisons, args.output)
        
        # Exit code
        if has_regression:
            print(f"\n⚠️  {len(checker.regressions)} regression(s) detected!")
            if args.fail_on_regression:
                sys.exit(1)
        else:
            print("\n✅ No regressions detected!")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()