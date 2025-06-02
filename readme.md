
# E-Commerce Analytics Pipeline: Most Expensive Products by Category

## 1. Data Source
We extracted product data from FakeStoreAPI:
- **Endpoint**: `https://fakestoreapi.com/products`
- **Data Format**: JSON with product details (price, category, title, etc.)
- **Sample Data**:
  ```json
  {
    "id": 1,
    "title": "Fjallraven Backpack",
    "price": 109.95,
    "category": "men's clothing",
    ...
  }
  ```

## 2. MapReduce Implementation
**Objective**: Find the most expensive product in each category

**Implementation Reference**:  
Based on [MRJob Beginner's Guide](https://medium.com/datable/beginners-guide-for-mapreduce-with-mrjob-in-python-dbd2e7dd0f86)

**Key Components**:
```python
# mapper.py
def mapper(self, _, line):
    product = json.loads(line)
    yield product['category'], (product['price'], product['title'])

# reducer.py
def reducer(self, category, products):
    yield category, max(products)
```

## 3. S3 Bucket Structure
```
s3://ha-doop/
├── input/               # Raw product data
│   └── products.json
├── scripts/             # Processing scripts
│   ├── job-runner.sh    # EMR step script
│   └── mapreduce.py     # MRJob implementation
└── deps/               # Dependencies
    └── requirements.txt
```

## 4. EMR Cluster Configuration
**Version**: EMR 7.1.0  
**Components**:
- Hadoop 3.3.4
- Spark 3.3.2
- Python 3.11 (native support)

**Cluster Setup**:
- **Instance Type**: m5.xlarge (1 instance only)
- **IAM Role**: `EMR_EC2_DefaultRole`
- **Bootstrap Action**: Installs dependencies from `deps/requirements.txt`

**Execution Flow**:
1. Cluster launches
2. Submits step pointing to `scripts/job-runner.sh`
3. Processes data → Outputs to `s3://ha-doop/output/`

## 5. FastAPI Dashboard (EC2)
**Instance Configuration**:
- **IAM Role**: `EMR_EC2_DefaultRole` (same as EMR)
- **Security Group**: Allows inbound on port 8000
- **Setup**:
  ```bash
  git clone <repo>
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  uvicorn main:app --host 0.0.0.0
  ```

**API Endpoints**:
| Endpoint | Description | Status |
|----------|-------------|--------|
| `GET /` | Homepage | ✅ Working |
| `GET /resultados` | View processed results | ✅ Working |
| `GET /descargar` | Download results as CSV | ✅ Working |
| `POST /procesar` | Trigger EMR job directly | ❌ Permission Issues |

## 6. Known Issues & Improvements
**Current Limitation**:
- `/procesar` endpoint fails due to missing `emr:AddJobFlowSteps` permission
![image](https://github.com/user-attachments/assets/c2ebf122-1d5b-48fe-9f75-56d7e0437d97)


**Recommended Fixes**:
1. Attach this policy to the EC2 instance role:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": "emr:AddJobFlowSteps",
       "Resource": "*"
     }]
   }
   ```










