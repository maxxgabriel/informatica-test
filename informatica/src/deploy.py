"""
Automated Deployment Module for Airflow DAGs
Handles CI/CD pipeline integration, DAG validation, and deployment orchestration
"""

import os
import sys
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import yaml
import json
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeploymentManager:
    """Manages automated deployment of Airflow DAGs and dependencies"""
    
    def __init__(self, config_path: str = "config/deploy_config.yaml"):
        """
        Initialize deployment manager
        
        Args:
            config_path: Path to deployment configuration file
        """
        self.config = self._load_config(config_path)
        self.deployment_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.deployment_log = []
        
    def _load_config(self, config_path: str) -> Dict:
        """Load deployment configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration: {e}")
            raise
    
    def validate_dags(self, dag_folder: str) -> Tuple[bool, List[str]]:
        """
        Validate all DAGs in the specified folder
        
        Args:
            dag_folder: Path to folder containing DAG files
            
        Returns:
            Tuple of (validation_success, list_of_errors)
        """
        logger.info(f"Validating DAGs in {dag_folder}")
        errors = []
        
        try:
            # Load DAGs using Airflow's DagBag
            dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
            
            # Check for import errors
            if dagbag.import_errors:
                for file_path, error in dagbag.import_errors.items():
                    error_msg = f"Import error in {file_path}: {error}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            # Validate each DAG
            for dag_id, dag in dagbag.dags.items():
                logger.info(f"Validating DAG: {dag_id}")
                
                # Check for cycles
                try:
                    check_cycle(dag)
                except Exception as e:
                    error_msg = f"Cycle detected in DAG {dag_id}: {e}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                
                # Validate task dependencies
                for task in dag.tasks:
                    if not task.task_id:
                        error_msg = f"Task without ID in DAG {dag_id}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Check for required parameters
                if not dag.default_args:
                    logger.warning(f"DAG {dag_id} has no default_args")
                
                if not dag.schedule_interval:
                    logger.warning(f"DAG {dag_id} has no schedule_interval")
            
            validation_success = len(errors) == 0
            
            if validation_success:
                logger.info(f"✓ All {len(dagbag.dags)} DAGs validated successfully")
            else:
                logger.error(f"✗ Validation failed with {len(errors)} errors")
            
            return validation_success, errors
            
        except Exception as e:
            error_msg = f"Unexpected error during validation: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            return False, errors
    
    def run_unit_tests(self, test_path: str = "tests/") -> bool:
        """
        Run unit tests for DAGs and supporting modules
        
        Args:
            test_path: Path to test directory
            
        Returns:
            True if all tests pass, False otherwise
        """
        logger.info(f"Running unit tests from {test_path}")
        
        try:
            result = subprocess.run(
                [
                    "pytest",
                    test_path,
                    "-v",
                    "--cov=src",
                    "--cov-report=html",
                    "--cov-report=term",
                    f"--html=reports/test_report_{self.deployment_id}.html",
                    "--self-contained-html"
                ],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            logger.info(result.stdout)
            
            if result.returncode == 0:
                logger.info("✓ All tests passed")
                return True
            else:
                logger.error("✗ Tests failed")
                logger.error(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("Tests timed out after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"Error running tests: {e}")
            return False
    
    def backup_current_deployment(self, airflow_home: str) -> str:
        """
        Create backup of current DAGs before deployment
        
        Args:
            airflow_home: Path to Airflow home directory
            
        Returns:
            Path to backup directory
        """
        logger.info("Creating backup of current deployment")
        
        backup_dir = Path(self.config['deployment']['backup_dir'])
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        backup_path = backup_dir / f"backup_{self.deployment_id}"
        
        try:
            dags_folder = Path(airflow_home) / "dags"
            
            if dags_folder.exists():
                shutil.copytree(dags_folder, backup_path)
                logger.info(f"✓ Backup created at {backup_path}")
            else:
                logger.warning(f"DAGs folder not found: {dags_folder}")
            
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            raise
    
    def deploy_dags(
        self,
        source_dir: str,
        target_dir: str,
        dry_run: bool = False
    ) -> bool:
        """
        Deploy DAG files to Airflow DAGs directory
        
        Args:
            source_dir: Source directory containing DAG files
            target_dir: Target Airflow DAGs directory
            dry_run: If True, only simulate deployment
            
        Returns:
            True if deployment successful, False otherwise
        """
        logger.info(f"Deploying DAGs from {source_dir} to {target_dir}")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        try:
            source_path = Path(source_dir)
            target_path = Path(target_dir)
            
            # Create target directory if it doesn't exist
            if not dry_run:
                target_path.mkdir(parents=True, exist_ok=True)
            
            # Copy DAG files
            dag_files = list(source_path.glob("dag_*.py"))
            
            for dag_file in dag_files:
                target_file = target_path / dag_file.name
                
                if dry_run:
                    logger.info(f"Would copy: {dag_file} -> {target_file}")
                else:
                    shutil.copy2(dag_file, target_file)
                    logger.info(f"✓ Deployed: {dag_file.name}")
            
            # Deploy supporting modules
            if (source_path / "modules").exists():
                target_modules = target_path / "modules"
                
                if dry_run:
                    logger.info(f"Would copy modules directory")
                else:
                    if target_modules.exists():
                        shutil.rmtree(target_modules)
                    shutil.copytree(source_path / "modules", target_modules)
                    logger.info("✓ Deployed supporting modules")
            
            logger.info(f"✓ Deployment completed: {len(dag_files)} DAG files")
            return True
            
        except Exception as e:
            logger.error(f"Error during deployment: {e}")
            return False
    
    def rollback_deployment(self, backup_path: str, target_dir: str) -> bool:
        """
        Rollback to previous deployment
        
        Args:
            backup_path: Path to backup directory
            target_dir: Target Airflow DAGs directory
            
        Returns:
            True if rollback successful, False otherwise
        """
        logger.warning(f"Rolling back deployment from {backup_path}")
        
        try:
            target_path = Path(target_dir)
            backup = Path(backup_path)
            
            if not backup.exists():
                logger.error(f"Backup not found: {backup_path}")
                return False
            
            # Remove current deployment
            if target_path.exists():
                shutil.rmtree(target_path)
            
            # Restore backup
            shutil.copytree(backup, target_path)
            
            logger.info("✓ Rollback completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during rollback: {e}")
            return False
    
    def install_dependencies(self, requirements_file: str = "requirements.txt") -> bool:
        """
        Install Python dependencies required for DAGs
        
        Args:
            requirements_file: Path to requirements.txt file
            
        Returns:
            True if installation successful, False otherwise
        """
        logger.info(f"Installing dependencies from {requirements_file}")
        
        try:
            result = subprocess.run(
                ["pip", "install", "-r", requirements_file],
                capture_output=True,
                text=True,
                timeout=600
            )
            
            if result.returncode == 0:
                logger.info("✓ Dependencies installed successfully")
                return True
            else:
                logger.error("✗ Failed to install dependencies")
                logger.error(result.stderr)
                return False
                
        except Exception as e:
            logger.error(f"Error installing dependencies: {e}")
            return False
    
    def verify_deployment(self, target_dir: str) -> bool:
        """
        Verify deployed DAGs are loadable and valid
        
        Args:
            target_dir: Target Airflow DAGs directory
            
        Returns:
            True if verification successful, False otherwise
        """
        logger.info("Verifying deployed DAGs")
        
        # Wait for Airflow to pick up new DAGs
        import time
        time.sleep(5)
        
        success, errors = self.validate_dags(target_dir)
        
        if success:
            logger.info("✓ Deployment verification successful")
        else:
            logger.error("✗ Deployment verification failed")
            for error in errors:
                logger.error(f"  - {error}")
        
        return success
    
    def generate_deployment_report(self, output_file: str = None) -> Dict:
        """
        Generate deployment report with metrics and status
        
        Args:
            output_file: Optional file path to save report
            
        Returns:
            Dictionary containing deployment report
        """
        report = {
            "deployment_id": self.deployment_id,
            "timestamp": datetime.now().isoformat(),
            "status": "completed" if self.deployment_log else "pending",
            "log": self.deployment_log,
            "summary": {
                "total_steps": len(self.deployment_log),
                "successful_steps": sum(
                    1 for log in self.deployment_log if log.get("status") == "success"
                ),
                "failed_steps": sum(
                    1 for log in self.deployment_log if log.get("status") == "failed"
                )
            }
        }
        
        if output_file:
            try:
                with open(output_file, 'w') as f:
                    json.dump(report, f, indent=2)
                logger.info(f"✓ Deployment report saved to {output_file}")
            except Exception as e:
                logger.error(f"Error saving deployment report: {e}")
        
        return report
    
    def execute_deployment_pipeline(
        self,
        source_dir: str = "dags/",
        dry_run: bool = False
    ) -> bool:
        """
        Execute complete deployment pipeline
        
        Args:
            source_dir: Source directory containing DAGs
            dry_run: If True, only simulate deployment
            
        Returns:
            True if pipeline successful, False otherwise
        """
        logger.info("=" * 60)
        logger.info("Starting Deployment Pipeline")
        logger.info("=" * 60)
        
        pipeline_success = True
        backup_path = None
        
        try:
            # Step 1: Install dependencies
            step = {"step": "install_dependencies", "timestamp": datetime.now().isoformat()}
            if self.install_dependencies():
                step["status"] = "success"
            else:
                step["status"] = "failed"
                pipeline_success = False
            self.deployment_log.append(step)
            
            if not pipeline_success:
                return False
            
            # Step 2: Run unit tests
            step = {"step": "run_tests", "timestamp": datetime.now().isoformat()}
            if self.run_unit_tests():
                step["status"] = "success"
            else:
                step["status"] = "failed"
                pipeline_success = False
            self.deployment_log.append(step)
            
            if not pipeline_success:
                return False
            
            # Step 3: Validate DAGs
            step = {"step": "validate_dags", "timestamp": datetime.now().isoformat()}
            success, errors = self.validate_dags(source_dir)
            if success:
                step["status"] = "success"
            else:
                step["status"] = "failed"
                step["errors"] = errors
                pipeline_success = False
            self.deployment_log.append(step)
            
            if not pipeline_success:
                return False
            
            # Step 4: Backup current deployment
            if not dry_run:
                step = {"step": "backup", "timestamp": datetime.now().isoformat()}
                try:
                    airflow_home = self.config['airflow']['airflow_home']
                    backup_path = self.backup_current_deployment(airflow_home)
                    step["status"] = "success"
                    step["backup_path"] = backup_path
                except Exception as e:
                    step["status"] = "failed"
                    step["error"] = str(e)
                    pipeline_success = False
                self.deployment_log.append(step)
                
                if not pipeline_success:
                    return False
            
            # Step 5: Deploy DAGs
            step = {"step": "deploy", "timestamp": datetime.now().isoformat()}
            target_dir = self.config['airflow']['dags_folder']
            if self.deploy_dags(source_dir, target_dir, dry_run):
                step["status"] = "success"
            else:
                step["status"] = "failed"
                pipeline_success = False
            self.deployment_log.append(step)
            
            if not pipeline_success and backup_path and not dry_run:
                # Rollback on failure
                logger.error("Deployment failed, initiating rollback")
                self.rollback_deployment(backup_path, target_dir)
                return False
            
            # Step 6: Verify deployment
            if not dry_run:
                step = {"step": "verify", "timestamp": datetime.now().isoformat()}
                if self.verify_deployment(target_dir):
                    step["status"] = "success"
                else:
                    step["status"] = "failed"
                    pipeline_success = False
                    # Rollback on verification failure
                    if backup_path:
                        self.rollback_deployment(backup_path, target_dir)
                self.deployment_log.append(step)
            
            # Generate deployment report
            report_file = f"reports/deployment_report_{self.deployment_id}.json"
            self.generate_deployment_report(report_file)
            
            if pipeline_success:
                logger.info("=" * 60)
                logger.info("✓ DEPLOYMENT PIPELINE COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
            else:
                logger.error("=" * 60)
                logger.error("✗ DEPLOYMENT PIPELINE FAILED")
                logger.error("=" * 60)
            
            return pipeline_success
            
        except Exception as e:
            logger.error(f"Unexpected error in deployment pipeline: {e}")
            if backup_path and not dry_run:
                self.rollback_deployment(backup_path, target_dir)
            return False


def main():
    """Main entry point for deployment script"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deploy Airflow DAGs")
    parser.add_argument(
        "--source-dir",
        default="dags/",
        help="Source directory containing DAG files"
    )
    parser.add_argument(
        "--config",
        default="config/deploy_config.yaml",
        help="Path to deployment configuration file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate deployment without making changes"
    )
    
    args = parser.parse_args()
    
    try:
        deployer = DeploymentManager(config_path=args.config)
        success = deployer.execute_deployment_pipeline(
            source_dir=args.source_dir,
            dry_run=args.dry_run
        )
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()