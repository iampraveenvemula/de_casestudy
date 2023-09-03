# Newyork City Job Postings - Data Engineering Challenge

The **de_casestudy** repository is a data engineering case study that focuses on leveraging PySpark to clean, transform, and analyze the New York City job posting dataset.

## Table of Contents

- [Project Structure](#project-structure)
- [Installation](#installation)
- [Tasks Performed](#tasks)
- [Asssumptions](#assumptions)
- [Deployment Steps](#deployment)

<a name="project-structure"></a>
## Project Structure

Outline the directory structure and key files in the repository. This section helps users understand how the project is organized.

	-- dataset
		|-- raw-data # contains raw data files shared
		|-- processed_data # contains the cleaned & processed datasets
	-- jupyter
		|-- notebook
			|-- main # contains the notebooks code for the key activities like data profiling, data processing, feature engineering and kpi generation.
			|-- utils # contains the notebooks for reusable components for the main activities
			|-- tests # contains the unit tests
	-- kpi_plots  # contains the saved kpi graphs generated from the main code 


<a name="installation"></a>
## Installation

A spark cluster is created with 1 master and 2 workers as docker containers running locally in my windows laptop using the installation instructions mentioned in the INSTALL.md file

<a name="tasks"></a>
## Tasks Performed

- Provided detailed analysis of data in the [Data Profiling](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/main/1.data_profiling.ipynb) code
	The resuable helper modules for data profiling: [click here](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/utils/profiling_utils.ipynb)
- Written a sample testcase for the activity 
- Performed data cleaning and feature engineering, extracted 3 key features utilized in KPI analysis. 
	Features required for KPI analysis are selected and stored under processed data. 
	code file: [Feature Engineering](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/main/2.data_preprocessing_feature_extraction.ipynb)
	The resuable helper modules for data preparation: [click here](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/utils/prep_utils.ipynb)

		Features Extracted:
			 1. Annual Salary From & Annual Salary To & Average Annual Salary: The salary column is not uniform, salary frequency is different across records, standardized salary from and salary to columns to annual level and derived Average Annual Salary for each posting.
			 2. Degree List & Highest Degree: From the Minimum Qual Requirements column, extracted list of available degrees like masters, pg, diploma, baccalaureate, high school etc and Since some postings have multiple degrees, Highest Degree column is extracted
			 3. Skills as ngrams: from Preferred SKills column, extracted skills information by performing text cleaning, transformations using nltk library


- The KPI analysis is done under the file: [KPI Analysis](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/main/3.kpi_analysis.ipynb)
  The results & plots are saved under the [kpi_plots](https://github.com/iampraveenvemula/de_casestudy/tree/main/kpi_plots) directory
  The resuable helper modules for kpi analysis: [click here](https://github.com/iampraveenvemula/de_casestudy/blob/main/jupyter/notebook/utils/kpi_utils.ipynb)
  
<a name="assumptions"></a>
## Assumptions

1. It was noticed from data profiling that the data file needs to be reloaded & escape the character '"'
2. There are duplicate jobIDs found, hence removed for further analysis.
3. The skills are assumed to be ngrams, and a human inspection is required for shortlist the ngrams that are skills to perform any analysis wrt skills column. Ideally, skills need to be extracted using data science techniques like named entity extraction.

<a name="deployment"></a>
## Deployment Steps using CI/CD

The steps below outlines a basic Continuous Integration/Continuous Deployment (CI/CD) cycle for deploying your PySpark code from a GitHub repository to an existing Spark cluster. The goal is to automate the deployment process to ensure code consistency and reliability.

### Prerequisites

- A GitHub repository containing PySpark code.
- A Spark cluster already set up and running.
- A CI/CD tool (e.g., Jenkins, GitLab CI/CD, GitHub Actions) configured to access your repository.
- Necessary cluster configurations and access permissions in place.

### CI/CD Steps:

#### 1. Code Repository 
Ensure your PySpark code is hosted in a version-controlled GitHub repository.

#### 2. Continuous Integration (CI) Pipeline

Set up the CI pipeline to trigger on code changes (e.g., commits to the `main` branch).

#### 3. Build Stage

Within the CI pipeline, create a build stage:

- In this stage, clone the GitHub repository to the CI/CD runner.
- Set up a virtual environment.
- Install Python dependencies from the `requirements.txt` file.
- Run unit tests and perform code quality checks (e.g., linting).

#### 4. Artifact Generation

Generate any necessary artifacts, such as data profiling reports, feature-engineered data, or plots, during the build stage.

#### 5. Continuous Deployment (CD) Pipeline

Set up the CD pipeline to trigger after a successful build.

#### 6. Deployment Stage

Within the CD pipeline, create a deployment stage:

- Use the `spark-submit` command to submit your PySpark job to the Spark cluster. Specify the Spark cluster's master URL and any configuration files.

Example:
```shell
spark-submit --master <master-url> --conf <config-file> your_main_script.py

#### 7. Result Storage

Store the data profiling reports, feature-engineered datasets, and kpi plots into the appropriate data sources that are easily & securely available for downstream applications.

#### 8. Scheduling with Apache Airflow

To automate the execution of the PySpark data pipelines on a scheduled basis, you can leverage Apache Airflow, an open-source workflow automation tool. Airflow allows us to define, schedule, and monitor your data pipeline tasks with ease.