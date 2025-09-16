# Use AWS Lambda Python 3.11 base image
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ${LAMBDA_TASK_ROOT}/

# Set proper permissions for Lambda execution
RUN chmod -R 755 ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["lambda_handler.lambda_handler"]