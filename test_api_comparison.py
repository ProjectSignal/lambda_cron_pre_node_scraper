#!/usr/bin/env python3
"""
Comprehensive API Comparison Testing for Pre-Node Scraper Lambda Processor

This script performs real-world API testing and comparison between different node data providers,
providing detailed quality scoring, field coverage analysis, and comprehensive reporting.

Usage:
    python test_api_comparison.py --test-usernames "john-doe,jane-smith" --providers "rapidapi,scrapfly"
    python test_api_comparison.py --live-test --sample-size 5
    python test_api_comparison.py --quality-analysis --min-score 80
"""

import json
import os
import sys
import argparse
import datetime
import time
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict
import statistics

# Add the current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import config
from external_apis import ProfileAPIManager, api_manager
from data_transformer import DataTransformer, validate_provider_data, calculate_quality_score
from clients import get_clients
from utils import get_logger, Timer
from errors import error_handler


class NodeDataComparator:
    """Advanced comparison system for node data from different providers"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.api_manager = api_manager
        self.transformer = DataTransformer()
        self.node_repo = get_clients().nodes

        # Node-specific critical fields for LinkedIn profiles
        self.critical_fields = [
            'linkedinHeadline', 'about', 'currentLocation', 'workExperience', 
            'education', 'skills', 'avatarURL', 'contacts'
        ]
        
        # Node-specific important fields
        self.important_fields = [
            'backgroundImage', 'accomplishments'
        ]
        
        # Node-specific quality weights (sum to 100)
        self.field_weights = {
            'linkedinHeadline': 20,
            'about': 20,
            'workExperience': 20,
            'education': 15,
            'skills': 10,
            'currentLocation': 5,
            'avatarURL': 5,
            'contacts': 3,
            'backgroundImage': 1,
            'accomplishments': 1
        }
    
    def compare_providers(self, test_usernames: List[str], providers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Compare multiple providers for the given test usernames
        
        Args:
            test_usernames: List of LinkedIn usernames to test
            providers: List of providers to compare (None for all available)
        
        Returns:
            Comprehensive comparison report
        """
        if providers is None:
            providers = self.api_manager.get_available_providers()
        
        self.logger.info(f"Starting provider comparison for {len(test_usernames)} usernames across {len(providers)} providers")
        
        results = {
            'test_metadata': {
                'test_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'test_usernames': test_usernames,
                'providers_tested': providers,
                'total_tests': len(test_usernames) * len(providers)
            },
            'provider_results': {},
            'comparative_analysis': {},
            'quality_summary': {},
            'field_coverage_analysis': {},
            'recommendations': []
        }
        
        # Collect data from all providers
        provider_data = {}
        for provider in providers:
            provider_data[provider] = self._test_provider(provider, test_usernames)
            results['provider_results'][provider] = provider_data[provider]
        
        # Perform comparative analysis
        results['comparative_analysis'] = self._analyze_provider_comparison(provider_data, test_usernames)
        results['quality_summary'] = self._generate_quality_summary(provider_data)
        results['field_coverage_analysis'] = self._analyze_field_coverage(provider_data)
        results['recommendations'] = self._generate_recommendations(provider_data)
        
        return results
    
    def _test_provider(self, provider: str, usernames: List[str]) -> Dict[str, Any]:
        """Test a single provider with multiple usernames"""
        self.logger.info(f"Testing provider: {provider}")
        
        provider_client = self.api_manager.get_provider(provider)
        if not provider_client:
            return {
                'provider_name': provider,
                'status': 'unavailable',
                'error': 'Provider not configured or available',
                'test_results': {}
            }
        
        # Test connection first
        connection_test = provider_client.test_connection()
        if not connection_test:
            return {
                'provider_name': provider,
                'status': 'connection_failed',
                'error': 'Provider connection test failed',
                'test_results': {}
            }
        
        test_results = {}
        success_count = 0
        total_quality_score = 0
        response_times = []
        
        for username in usernames:
            self.logger.info(f"Testing {provider} with username: {username}")
            
            start_time = time.time()
            try:
                # Fetch raw data
                with Timer(f"{provider} API call for {username}", self.logger):
                    raw_data = provider_client.fetch(username)
                
                response_time = time.time() - start_time
                response_times.append(response_time)
                
                if raw_data:
                    # Transform data
                    transformed_data = self.transformer.transform_data(raw_data, provider)
                    
                    if transformed_data:
                        # Validate and score
                        validation_result = validate_provider_data(transformed_data, provider)
                        quality_score = validation_result.get('quality_score', 0)
                        total_quality_score += quality_score
                        success_count += 1
                        
                        test_results[username] = {
                            'status': 'success',
                            'response_time': response_time,
                            'quality_score': quality_score,
                            'validation_passed': validation_result.get('valid', False),
                            'data_summary': self._summarize_node_data(transformed_data),
                            'raw_data_keys': list(raw_data.keys()) if isinstance(raw_data, dict) else [],
                            'transformed_data_keys': list(transformed_data.keys()),
                            'critical_fields_present': self._count_critical_fields(transformed_data),
                            'field_completeness': self._calculate_field_completeness(transformed_data)
                        }
                    else:
                        test_results[username] = {
                            'status': 'transformation_failed',
                            'response_time': response_time,
                            'error': 'Data transformation failed',
                            'raw_data_available': raw_data is not None
                        }
                else:
                    test_results[username] = {
                        'status': 'no_data',
                        'response_time': response_time,
                        'error': 'No data returned from API'
                    }
                    
            except Exception as e:
                response_time = time.time() - start_time
                test_results[username] = {
                    'status': 'error',
                    'response_time': response_time,
                    'error': str(e),
                    'exception_type': type(e).__name__
                }
                
                self.logger.error(f"Error testing {provider} with {username}: {e}")
        
        # Calculate provider statistics
        avg_quality_score = total_quality_score / success_count if success_count > 0 else 0
        avg_response_time = statistics.mean(response_times) if response_times else 0
        success_rate = success_count / len(usernames)
        
        return {
            'provider_name': provider,
            'status': 'completed',
            'test_results': test_results,
            'statistics': {
                'success_count': success_count,
                'total_tests': len(usernames),
                'success_rate': success_rate,
                'average_quality_score': avg_quality_score,
                'average_response_time': avg_response_time,
                'min_response_time': min(response_times) if response_times else 0,
                'max_response_time': max(response_times) if response_times else 0
            }
        }
    
    def _summarize_node_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of node data for comparison"""
        summary = {
            'has_headline': bool(data.get('linkedinHeadline')),
            'has_about': bool(data.get('about')),
            'has_location': bool(data.get('currentLocation')),
            'has_avatar': bool(data.get('avatarURL')),
            'has_background_image': bool(data.get('backgroundImage')),
            'work_experience_count': len(data.get('workExperience', [])),
            'education_count': len(data.get('education', [])),
            'skills_count': len(data.get('skills', [])),
            'accomplishments_sections': len(data.get('accomplishments', {})) if isinstance(data.get('accomplishments'), dict) else 0,
            'contacts_available': bool(data.get('contacts', {}).get('linkedin')),
            'total_fields': len([v for v in data.values() if v and str(v).strip()]),
            'api_scraped': data.get('apiScraped', False),
            'quality_score': data.get('quality_score', 0)
        }
        
        return summary
    
    def _count_critical_fields(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Count presence of critical fields"""
        critical_status = {}
        
        for field in self.critical_fields:
            if field in ['workExperience', 'education', 'skills']:
                critical_status[field] = bool(data.get(field) and len(data.get(field, [])) > 0)
            elif field == 'contacts':
                critical_status[field] = bool(data.get('contacts', {}).get('linkedin'))
            else:
                critical_status[field] = bool(data.get(field) and str(data.get(field)).strip())
        
        return critical_status
    
    def _calculate_field_completeness(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate field completeness using node-specific weights"""
        completeness_score = 0
        field_status = {}
        
        for field, weight in self.field_weights.items():
            is_present = False
            
            if field in ['workExperience', 'education', 'skills']:
                is_present = bool(data.get(field) and len(data.get(field, [])) > 0)
                field_status[field] = {
                    'present': is_present,
                    'weight': weight,
                    'count': len(data.get(field, [])) if is_present else 0
                }
            elif field == 'contacts':
                is_present = bool(data.get('contacts', {}).get('linkedin'))
                field_status[field] = {
                    'present': is_present,
                    'weight': weight,
                    'linkedin_url': data.get('contacts', {}).get('linkedin') if is_present else None
                }
            elif field == 'accomplishments':
                is_present = bool(data.get(field) and isinstance(data.get(field), dict) and data.get(field))
                field_status[field] = {
                    'present': is_present,
                    'weight': weight,
                    'sections': list(data.get(field, {}).keys()) if is_present else []
                }
            else:
                is_present = bool(data.get(field) and str(data.get(field)).strip())
                field_status[field] = {
                    'present': is_present,
                    'weight': weight,
                    'value_length': len(str(data.get(field, ''))) if is_present else 0
                }
            
            if is_present:
                completeness_score += weight
        
        return {
            'completeness_score': completeness_score,
            'max_possible_score': sum(self.field_weights.values()),
            'completeness_percentage': (completeness_score / sum(self.field_weights.values())) * 100,
            'field_status': field_status
        }
    
    def _analyze_provider_comparison(self, provider_data: Dict[str, Dict], usernames: List[str]) -> Dict[str, Any]:
        """Analyze comparative performance between providers"""
        comparison = {
            'provider_rankings': {},
            'field_coverage_comparison': {},
            'quality_score_comparison': {},
            'response_time_comparison': {},
            'success_rate_comparison': {},
            'per_username_analysis': {}
        }
        
        # Provider rankings by different metrics
        providers = list(provider_data.keys())
        available_providers = [p for p in providers if provider_data[p].get('status') == 'completed']
        
        if not available_providers:
            return comparison
        
        # Quality score rankings
        quality_scores = {}
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            quality_scores[provider] = stats.get('average_quality_score', 0)
        
        comparison['provider_rankings']['by_quality_score'] = sorted(
            quality_scores.items(), key=lambda x: x[1], reverse=True
        )
        
        # Response time rankings (lower is better)
        response_times = {}
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            response_times[provider] = stats.get('average_response_time', float('inf'))
        
        comparison['provider_rankings']['by_response_time'] = sorted(
            response_times.items(), key=lambda x: x[1]
        )
        
        # Success rate rankings
        success_rates = {}
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            success_rates[provider] = stats.get('success_rate', 0)
        
        comparison['provider_rankings']['by_success_rate'] = sorted(
            success_rates.items(), key=lambda x: x[1], reverse=True
        )
        
        # Per-username analysis
        for username in usernames:
            username_analysis = {'providers_succeeded': [], 'quality_scores': {}, 'best_provider': None}
            
            best_score = 0
            best_provider = None
            
            for provider in available_providers:
                test_results = provider_data[provider].get('test_results', {})
                if username in test_results and test_results[username].get('status') == 'success':
                    username_analysis['providers_succeeded'].append(provider)
                    quality_score = test_results[username].get('quality_score', 0)
                    username_analysis['quality_scores'][provider] = quality_score
                    
                    if quality_score > best_score:
                        best_score = quality_score
                        best_provider = provider
            
            username_analysis['best_provider'] = best_provider
            username_analysis['best_quality_score'] = best_score
            comparison['per_username_analysis'][username] = username_analysis
        
        return comparison
    
    def _generate_quality_summary(self, provider_data: Dict[str, Dict]) -> Dict[str, Any]:
        """Generate quality summary across all providers"""
        summary = {
            'overall_statistics': {},
            'quality_distribution': {},
            'field_completeness_stats': {}
        }
        
        all_quality_scores = []
        all_completeness_scores = []
        provider_quality_stats = {}
        
        for provider, data in provider_data.items():
            if data.get('status') != 'completed':
                continue
            
            test_results = data.get('test_results', {})
            provider_scores = []
            provider_completeness = []
            
            for username, result in test_results.items():
                if result.get('status') == 'success':
                    quality_score = result.get('quality_score', 0)
                    provider_scores.append(quality_score)
                    all_quality_scores.append(quality_score)
                    
                    # Extract completeness score if available
                    field_completeness = result.get('field_completeness', {})
                    completeness_percentage = field_completeness.get('completeness_percentage', 0)
                    provider_completeness.append(completeness_percentage)
                    all_completeness_scores.append(completeness_percentage)
            
            if provider_scores:
                provider_quality_stats[provider] = {
                    'mean_quality': statistics.mean(provider_scores),
                    'median_quality': statistics.median(provider_scores),
                    'min_quality': min(provider_scores),
                    'max_quality': max(provider_scores),
                    'std_dev_quality': statistics.stdev(provider_scores) if len(provider_scores) > 1 else 0,
                    'mean_completeness': statistics.mean(provider_completeness) if provider_completeness else 0,
                    'total_successful_tests': len(provider_scores)
                }
        
        if all_quality_scores:
            summary['overall_statistics'] = {
                'total_successful_tests': len(all_quality_scores),
                'mean_quality_all_providers': statistics.mean(all_quality_scores),
                'median_quality_all_providers': statistics.median(all_quality_scores),
                'quality_std_dev': statistics.stdev(all_quality_scores) if len(all_quality_scores) > 1 else 0,
                'min_quality_observed': min(all_quality_scores),
                'max_quality_observed': max(all_quality_scores),
                'mean_completeness_all_providers': statistics.mean(all_completeness_scores) if all_completeness_scores else 0
            }
            
            # Quality distribution
            quality_ranges = {
                'excellent (90-100)': len([s for s in all_quality_scores if s >= 90]),
                'good (70-89)': len([s for s in all_quality_scores if 70 <= s < 90]),
                'fair (50-69)': len([s for s in all_quality_scores if 50 <= s < 70]),
                'poor (0-49)': len([s for s in all_quality_scores if s < 50])
            }
            
            summary['quality_distribution'] = quality_ranges
        
        summary['provider_quality_stats'] = provider_quality_stats
        
        return summary
    
    def _analyze_field_coverage(self, provider_data: Dict[str, Dict]) -> Dict[str, Any]:
        """Analyze field coverage across providers for node data"""
        field_analysis = {
            'critical_fields_coverage': {},
            'important_fields_coverage': {},
            'provider_field_strength': {},
            'field_consistency': {}
        }
        
        # Count field coverage for critical fields
        for field in self.critical_fields:
            field_coverage = {}
            for provider, data in provider_data.items():
                if data.get('status') != 'completed':
                    continue
                
                test_results = data.get('test_results', {})
                total_tests = len([r for r in test_results.values() if r.get('status') == 'success'])
                field_present_count = 0
                
                for username, result in test_results.items():
                    if result.get('status') == 'success':
                        critical_fields = result.get('critical_fields_present', {})
                        if critical_fields.get(field, False):
                            field_present_count += 1
                
                coverage_percentage = (field_present_count / total_tests * 100) if total_tests > 0 else 0
                field_coverage[provider] = {
                    'present_count': field_present_count,
                    'total_tests': total_tests,
                    'coverage_percentage': coverage_percentage
                }
            
            field_analysis['critical_fields_coverage'][field] = field_coverage
        
        # Analyze provider field strengths
        for provider, data in provider_data.items():
            if data.get('status') != 'completed':
                continue
            
            test_results = data.get('test_results', {})
            provider_strengths = {}
            
            for field in self.critical_fields + self.important_fields:
                field_count = 0
                total_count = 0
                
                for username, result in test_results.items():
                    if result.get('status') == 'success':
                        total_count += 1
                        critical_fields = result.get('critical_fields_present', {})
                        if critical_fields.get(field, False):
                            field_count += 1
                
                if total_count > 0:
                    provider_strengths[field] = {
                        'coverage_percentage': (field_count / total_count) * 100,
                        'absolute_count': field_count
                    }
            
            field_analysis['provider_field_strength'][provider] = provider_strengths
        
        return field_analysis
    
    def _generate_recommendations(self, provider_data: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on test results"""
        recommendations = []
        
        # Analyze provider availability
        available_providers = [p for p, d in provider_data.items() if d.get('status') == 'completed']
        unavailable_providers = [p for p, d in provider_data.items() if d.get('status') != 'completed']
        
        if unavailable_providers:
            recommendations.append({
                'type': 'configuration',
                'severity': 'high',
                'title': 'Provider Configuration Issues',
                'description': f"Providers not available: {', '.join(unavailable_providers)}",
                'action': 'Check API keys and provider configuration for unavailable providers'
            })
        
        if not available_providers:
            recommendations.append({
                'type': 'critical',
                'severity': 'critical',
                'title': 'No Working Providers',
                'description': 'No providers are currently functional',
                'action': 'Fix provider configuration immediately - no node processing possible'
            })
            return recommendations
        
        # Quality analysis
        quality_scores = []
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            avg_quality = stats.get('average_quality_score', 0)
            quality_scores.append((provider, avg_quality))
        
        quality_scores.sort(key=lambda x: x[1], reverse=True)
        
        if quality_scores:
            best_provider, best_score = quality_scores[0]
            
            if best_score < 70:
                recommendations.append({
                    'type': 'quality',
                    'severity': 'medium',
                    'title': 'Low Overall Quality Scores',
                    'description': f'Best provider ({best_provider}) only achieves {best_score:.1f} average quality',
                    'action': 'Review data transformation logic and consider additional data sources'
                })
            
            # Provider ranking recommendation
            if len(available_providers) > 1:
                recommendations.append({
                    'type': 'optimization',
                    'severity': 'low',
                    'title': 'Optimal Provider Order',
                    'description': f'Recommended fallback order: {" → ".join([p for p, _ in quality_scores])}',
                    'action': f'Set PROVIDER_FALLBACK_CHAIN="{",".join([p for p, _ in quality_scores])}"'
                })
        
        # Response time analysis
        response_times = []
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            avg_time = stats.get('average_response_time', 0)
            response_times.append((provider, avg_time))
        
        response_times.sort(key=lambda x: x[1])
        
        if response_times:
            slowest_provider, slowest_time = response_times[-1]
            if slowest_time > 10:  # More than 10 seconds
                recommendations.append({
                    'type': 'performance',
                    'severity': 'medium',
                    'title': 'Slow Provider Response',
                    'description': f'Provider {slowest_provider} averages {slowest_time:.1f}s response time',
                    'action': f'Consider reducing timeout for {slowest_provider} or deprioritizing in fallback chain'
                })
        
        # Success rate analysis
        for provider in available_providers:
            stats = provider_data[provider].get('statistics', {})
            success_rate = stats.get('success_rate', 0)
            
            if success_rate < 0.8:  # Less than 80% success rate
                recommendations.append({
                    'type': 'reliability',
                    'severity': 'medium',
                    'title': f'Low Success Rate for {provider}',
                    'description': f'Provider {provider} only succeeds {success_rate*100:.1f}% of the time',
                    'action': f'Investigate {provider} connection issues and error patterns'
                })
        
        return recommendations
    
    def generate_detailed_report(self, comparison_results: Dict[str, Any], output_file: Optional[str] = None) -> str:
        """Generate a detailed markdown report"""
        report_lines = [
            "# Node Data Provider Comparison Report",
            "",
            f"**Generated:** {comparison_results['test_metadata']['test_time']}",
            f"**Test Usernames:** {', '.join(comparison_results['test_metadata']['test_usernames'])}",
            f"**Providers Tested:** {', '.join(comparison_results['test_metadata']['providers_tested'])}",
            f"**Total Tests:** {comparison_results['test_metadata']['total_tests']}",
            "",
            "## Executive Summary",
            ""
        ]
        
        # Executive summary
        quality_summary = comparison_results.get('quality_summary', {})
        overall_stats = quality_summary.get('overall_statistics', {})
        
        if overall_stats:
            report_lines.extend([
                f"- **Average Quality Score:** {overall_stats.get('mean_quality_all_providers', 0):.1f}/100",
                f"- **Total Successful Tests:** {overall_stats.get('total_successful_tests', 0)}",
                f"- **Quality Range:** {overall_stats.get('min_quality_observed', 0):.1f} - {overall_stats.get('max_quality_observed', 0):.1f}",
                f"- **Average Field Completeness:** {overall_stats.get('mean_completeness_all_providers', 0):.1f}%",
                ""
            ])
        
        # Provider rankings
        comparison = comparison_results.get('comparative_analysis', {})
        rankings = comparison.get('provider_rankings', {})
        
        report_lines.extend([
            "## Provider Rankings",
            "",
            "### By Quality Score",
            ""
        ])
        
        quality_ranking = rankings.get('by_quality_score', [])
        for i, (provider, score) in enumerate(quality_ranking, 1):
            report_lines.append(f"{i}. **{provider}** - {score:.1f}/100")
        
        report_lines.extend([
            "",
            "### By Response Time",
            ""
        ])
        
        time_ranking = rankings.get('by_response_time', [])
        for i, (provider, time) in enumerate(time_ranking, 1):
            report_lines.append(f"{i}. **{provider}** - {time:.2f}s")
        
        report_lines.extend([
            "",
            "### By Success Rate",
            ""
        ])
        
        success_ranking = rankings.get('by_success_rate', [])
        for i, (provider, rate) in enumerate(success_ranking, 1):
            report_lines.append(f"{i}. **{provider}** - {rate*100:.1f}%")
        
        # Detailed provider results
        report_lines.extend([
            "",
            "## Detailed Provider Results",
            ""
        ])
        
        provider_results = comparison_results.get('provider_results', {})
        for provider, data in provider_results.items():
            report_lines.extend([
                f"### {provider.title()} Provider",
                ""
            ])
            
            if data.get('status') != 'completed':
                report_lines.extend([
                    f"**Status:** {data.get('status', 'unknown')}",
                    f"**Error:** {data.get('error', 'Unknown error')}",
                    ""
                ])
                continue
            
            stats = data.get('statistics', {})
            report_lines.extend([
                f"**Success Rate:** {stats.get('success_rate', 0)*100:.1f}%",
                f"**Average Quality Score:** {stats.get('average_quality_score', 0):.1f}/100",
                f"**Average Response Time:** {stats.get('average_response_time', 0):.2f}s",
                f"**Response Time Range:** {stats.get('min_response_time', 0):.2f}s - {stats.get('max_response_time', 0):.2f}s",
                ""
            ])
            
            # Per-username results
            test_results = data.get('test_results', {})
            if test_results:
                report_lines.extend([
                    "#### Individual Test Results",
                    "",
                    "| Username | Status | Quality Score | Response Time | Fields Present |",
                    "|----------|--------|---------------|---------------|----------------|"
                ])
                
                for username, result in test_results.items():
                    status = result.get('status', 'unknown')
                    quality = result.get('quality_score', 0)
                    response_time = result.get('response_time', 0)
                    critical_fields = result.get('critical_fields_present', {})
                    fields_count = sum(1 for v in critical_fields.values() if v)
                    
                    report_lines.append(
                        f"| {username} | {status} | {quality:.1f} | {response_time:.2f}s | {fields_count}/{len(self.critical_fields)} |"
                    )
                
                report_lines.append("")
        
        # Field coverage analysis
        field_coverage = comparison_results.get('field_coverage_analysis', {})
        critical_coverage = field_coverage.get('critical_fields_coverage', {})
        
        if critical_coverage:
            report_lines.extend([
                "## Critical Fields Coverage Analysis",
                "",
                "| Field | " + " | ".join(f"{p} Coverage" for p in comparison_results['test_metadata']['providers_tested']) + " |",
                "|-------|" + "|".join("----------" for _ in comparison_results['test_metadata']['providers_tested']) + "|"
            ])
            
            for field, providers_data in critical_coverage.items():
                row = f"| {field} |"
                for provider in comparison_results['test_metadata']['providers_tested']:
                    if provider in providers_data:
                        coverage = providers_data[provider].get('coverage_percentage', 0)
                        row += f" {coverage:.1f}% |"
                    else:
                        row += " N/A |"
                report_lines.append(row)
            
            report_lines.append("")
        
        # Recommendations
        recommendations = comparison_results.get('recommendations', [])
        if recommendations:
            report_lines.extend([
                "## Recommendations",
                ""
            ])
            
            for rec in recommendations:
                severity_emoji = {'critical': '🚨', 'high': '⚠️', 'medium': '📋', 'low': '💡'}.get(rec.get('severity'), '📝')
                report_lines.extend([
                    f"### {severity_emoji} {rec.get('title', 'Recommendation')} ({rec.get('severity', 'unknown')})",
                    "",
                    f"**Description:** {rec.get('description', 'No description')}",
                    f"**Action:** {rec.get('action', 'No action specified')}",
                    ""
                ])
        
        # Quality distribution
        quality_dist = quality_summary.get('quality_distribution', {})
        if quality_dist:
            report_lines.extend([
                "## Quality Distribution",
                ""
            ])
            
            total_tests = sum(quality_dist.values())
            for range_desc, count in quality_dist.items():
                percentage = (count / total_tests * 100) if total_tests > 0 else 0
                report_lines.append(f"- **{range_desc.title()}:** {count} tests ({percentage:.1f}%)")
            
            report_lines.append("")
        
        # Configuration recommendations
        report_lines.extend([
            "## Configuration Recommendations",
            "",
            "Based on this analysis, consider the following configuration updates:",
            ""
        ])
        
        # Optimal fallback chain
        if quality_ranking:
            optimal_chain = ",".join([p for p, _ in quality_ranking])
            report_lines.extend([
                "### Optimal Provider Fallback Chain",
                "",
                f"```bash",
                f"PROVIDER_FALLBACK_CHAIN=\"{optimal_chain}\"",
                f"```",
                ""
            ])
        
        # Quality threshold recommendations
        if overall_stats:
            median_quality = overall_stats.get('median_quality_all_providers', 70)
            recommended_threshold = max(50, int(median_quality * 0.8))  # 80% of median, minimum 50
            report_lines.extend([
                "### Quality Score Threshold",
                "",
                f"```bash",
                f"QUALITY_SCORE_THRESHOLD={recommended_threshold}",
                f"```",
                ""
            ])
        
        report_content = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_content)
            self.logger.info(f"Report saved to {output_file}")
        
        return report_content


def setup_test_environment():
    """Set up test environment with real credentials"""
    if not os.getenv("BASE_API_URL"):
        print("⚠️  Set BASE_API_URL to point at the Brace API service")

    if not os.getenv("INSIGHTS_API_KEY"):
        print("⚠️  Set INSIGHTS_API_KEY for API authentication")

    if not os.getenv("RAPIDAPI_KEY"):
        print("⚠️  Set RAPIDAPI_KEY for provider testing")

    if not os.getenv("RAPIDAPI_HOST"):
        print("⚠️  Set RAPIDAPI_HOST for provider testing")


def get_sample_usernames_from_api(node_repo, sample_size: int = 5) -> List[str]:
    """Get sample LinkedIn usernames via REST API for testing"""
    try:
        nodes = node_repo.scrape_candidates(limit=sample_size)
        usernames = [node.get('linkedinUsername') for node in nodes if node.get('linkedinUsername')]
        return usernames[:sample_size]
    except Exception as e:
        print(f"Error getting sample usernames from API: {e}")
        return []


def main():
    """Main test runner with comprehensive CLI options"""
    parser = argparse.ArgumentParser(description='Node Data Provider Comparison Testing')
    parser.add_argument('--test-usernames', type=str, 
                       help='Comma-separated list of LinkedIn usernames to test')
    parser.add_argument('--providers', type=str,
                       help='Comma-separated list of providers to test (default: all available)')
    parser.add_argument('--live-test', action='store_true',
                       help='Use live data from API for testing')
    parser.add_argument('--sample-size', type=int, default=5,
                       help='Number of sample usernames to use for live testing (default: 5)')
    parser.add_argument('--quality-analysis', action='store_true',
                       help='Focus on quality analysis and scoring')
    parser.add_argument('--min-score', type=int, default=70,
                       help='Minimum quality score threshold for analysis (default: 70)')
    parser.add_argument('--output-file', type=str,
                       help='Output file for detailed report (markdown format)')
    parser.add_argument('--json-output', type=str,
                       help='Output file for JSON results')
    
    args = parser.parse_args()
    
    print("🚀 Starting Node Data Provider Comparison Testing")
    print("=" * 60)
    
    setup_test_environment()
    
    # Initialize comparator
    comparator = NodeDataComparator()
    
    # Determine test usernames
    test_usernames = []
    
    if args.test_usernames:
        test_usernames = [u.strip() for u in args.test_usernames.split(',')]
        print(f"✅ Using provided test usernames: {test_usernames}")
    elif args.live_test and comparator.node_repo:
        test_usernames = get_sample_usernames_from_api(comparator.node_repo, args.sample_size)
        if test_usernames:
            print(f"✅ Using {len(test_usernames)} usernames from API: {test_usernames}")
        else:
            print("❌ No suitable usernames found via API")
            return
    else:
        # No default dummy usernames - require real data
        print("❌ No test usernames available for testing")
        print("   Options:")
        print("   1. Set --test-usernames 'username1,username2' for specific usernames")
        print("   2. Use --live-test for API sampling")
        print("   3. Set TEST_NODE_USERNAMES environment variable")
        return
    
    if not test_usernames:
        print("❌ No test usernames available")
        return
    
    # Determine providers to test
    providers_to_test = None
    if args.providers:
        providers_to_test = [p.strip() for p in args.providers.split(',')]
        print(f"✅ Testing specific providers: {providers_to_test}")
    else:
        available_providers = comparator.api_manager.get_available_providers()
        providers_to_test = available_providers
        print(f"✅ Testing all available providers: {providers_to_test}")
    
    if not providers_to_test:
        print("❌ No providers available for testing")
        return
    
    # Run comparison
    print(f"\n🧪 Running comprehensive comparison...")
    with Timer("Complete provider comparison", comparator.logger):
        results = comparator.compare_providers(test_usernames, providers_to_test)
    
    # Generate detailed report
    print(f"\n📊 Generating detailed report...")
    output_file = args.output_file or f"node_provider_comparison_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report = comparator.generate_detailed_report(results, output_file)
    
    # Save JSON output if requested
    if args.json_output:
        with open(args.json_output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"✅ JSON results saved to {args.json_output}")
    
    # Print summary to console
    print(f"\n📈 Test Results Summary:")
    print(f"=" * 40)
    
    quality_summary = results.get('quality_summary', {})
    overall_stats = quality_summary.get('overall_statistics', {})
    
    if overall_stats:
        print(f"Total Successful Tests: {overall_stats.get('total_successful_tests', 0)}")
        print(f"Average Quality Score: {overall_stats.get('mean_quality_all_providers', 0):.1f}/100")
        print(f"Quality Range: {overall_stats.get('min_quality_observed', 0):.1f} - {overall_stats.get('max_quality_observed', 0):.1f}")
        print(f"Average Field Completeness: {overall_stats.get('mean_completeness_all_providers', 0):.1f}%")
    
    # Provider rankings
    comparison = results.get('comparative_analysis', {})
    rankings = comparison.get('provider_rankings', {})
    quality_ranking = rankings.get('by_quality_score', [])
    
    if quality_ranking:
        print(f"\n🏆 Provider Rankings (by Quality Score):")
        for i, (provider, score) in enumerate(quality_ranking, 1):
            print(f"  {i}. {provider}: {score:.1f}/100")
    
    # Recommendations
    recommendations = results.get('recommendations', [])
    if recommendations:
        print(f"\n💡 Key Recommendations:")
        for rec in recommendations[:3]:  # Show top 3 recommendations
            severity_emoji = {'critical': '🚨', 'high': '⚠️', 'medium': '📋', 'low': '💡'}.get(rec.get('severity'), '📝')
            print(f"  {severity_emoji} {rec.get('title', 'Recommendation')}: {rec.get('action', 'No action specified')}")
    
    print(f"\n✅ Detailed report saved to: {output_file}")
    
    # Quality analysis focus
    if args.quality_analysis:
        print(f"\n🔍 Quality Analysis (Threshold: {args.min_score}):")
        
        quality_dist = quality_summary.get('quality_distribution', {})
        if quality_dist:
            total_tests = sum(quality_dist.values())
            above_threshold = sum(count for range_desc, count in quality_dist.items() 
                                if 'excellent' in range_desc or 'good' in range_desc)
            
            print(f"  Tests above {args.min_score}: {above_threshold}/{total_tests} ({above_threshold/total_tests*100:.1f}%)")
            
            for range_desc, count in quality_dist.items():
                percentage = (count / total_tests * 100) if total_tests > 0 else 0
                print(f"  {range_desc.title()}: {count} ({percentage:.1f}%)")
    
    print(f"\n🏁 Node Data Provider Comparison Complete!")
    
    # Cleanup


if __name__ == "__main__":
    main()