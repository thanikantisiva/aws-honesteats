"""Rider signup and authentication routes"""
import re
import os
import boto3
import base64
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.user_service import UserService
from services.rider_service import RiderService
from models.user import User
from models.rider import Rider
from utils.dynamodb import generate_id
from datetime import datetime

logger = Logger()
tracer = Tracer()
metrics = Metrics()

# S3 client for presigned URLs
s3_client = boto3.client('s3')
RIDER_DOCUMENTS_BUCKET = os.environ.get('RIDER_DOCUMENTS_BUCKET', 'rider-documents-dev')


def register_rider_signup_routes(app):
    """Register rider signup routes"""

    # @app.post(f"/api/v1/riders/documents/download/{}/{}")
    # @tracer.capture_method
    
    @app.post("/api/v1/riders/documents/upload")
    @tracer.capture_method
    def upload_document():
        """
        Upload document image to S3 (backend handles upload)
        
        Request body:
        {
            "phone": "9876543210",
            "documentType": "aadhar" | "pan",
            "imageBase64": "data:image/jpeg;base64,..."
        }
        
        Response:
        {
            "fileUrl": "https://bucket.s3.amazonaws.com/riders/9876543210/aadhar-timestamp.jpg"
        }
        """
        try:
            body = app.current_event.json_body
            
            phone = body.get('phone')
            document_type = body.get('documentType')
            image_base64 = body.get('imageBase64')
            
            # Validation
            if not phone:
                return {"error": "Phone number required"}, 400
            
            if document_type not in ['aadhar', 'pan']:
                return {"error": "documentType must be 'aadhar' or 'pan'"}, 400
            
            if not image_base64:
                return {"error": "imageBase64 required"}, 400
            
            # Remove data URI prefix if present
            if image_base64.startswith('data:'):
                image_base64 = image_base64.split(',')[1]
            
            # Decode base64
            image_data = base64.b64decode(image_base64)
            
            # Validate file size (max 10MB)
            max_size_mb = 10
            if len(image_data) > max_size_mb * 1024 * 1024:
                return {"error": f"File size exceeds {max_size_mb}MB limit"}, 400
            
            # Create unique file path
            timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
            file_key = f"riders/{phone}/{document_type}-{timestamp}.jpg"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=RIDER_DOCUMENTS_BUCKET,
                Key=file_key,
                Body=image_data,
                ContentType='image/jpeg',
                ContentDisposition='inline'
            )
            
            # Generate public URL
            file_url = f"https://{RIDER_DOCUMENTS_BUCKET}.s3.amazonaws.com/{file_key}"
            
            logger.info(f"Uploaded {document_type} document for {phone} to S3: {file_key}")
            metrics.add_metric(name="DocumentUploaded", unit="Count", value=1)
            
            return {
                "fileUrl": file_url
            }, 200
            
        except Exception as e:
            logger.error("Error uploading document to S3", exc_info=True)
            metrics.add_metric(name="DocumentUploadFailed", unit="Count", value=1)
            return {"error": "Failed to upload document", "message": str(e)}, 500
    
    @app.post("/api/v1/riders/signup")
    @tracer.capture_method
    def signup_rider():
        """
        Rider signup with KYC - Creates entries in both Users and Riders tables
        
        Flow:
        1. Generate rider_id
        2. Create entry in Users table (authentication, KYC, profile)
        3. Create entry in Riders table (operational data)
        
        Request body:
        {
            "phone": "9876543210",
            "firstName": "John",
            "lastName": "Doe",
            "address": "123 Main St, City",
            "aadharNumber": "123456789012",
            "aadharImageUrl": "https://bucket.s3.amazonaws.com/riders/9876543210/aadhar.jpg",
            "panNumber": "ABCDE1234F",
            "panImageUrl": "https://bucket.s3.amazonaws.com/riders/9876543210/pan.jpg"
        }
        """
        try:
            body = app.current_event.json_body
            
            # Validation - Updated to accept S3 URLs instead of base64
            required_fields = ['phone', 'firstName', 'lastName', 'address', 
                              'aadharNumber', 'aadharImageUrl', 
                              'panNumber', 'panImageUrl']
            
            for field in required_fields:
                if not body.get(field):
                    return {"error": f"Missing required field: {field}"}, 400
            
            phone = body['phone']
            
            # Check if rider role already exists (customer role is OK)
            existing_rider = UserService.get_user_by_role(phone, "RIDER")
            if existing_rider:
                if existing_rider.rider_status == User.RIDER_STATUS_SIGNUP_DONE:
                    return {
                        "message": "Your application is under review",
                        "status": "SIGNUP_DONE",
                        "riderId": existing_rider.rider_id
                    }, 200
                elif existing_rider.rider_status == User.RIDER_STATUS_APPROVED:
                    return {"error": "Phone number already registered as rider"}, 400
            
            # Validate Aadhar
            if not re.match(r'^\d{12}$', body['aadharNumber']):
                return {"error": "Invalid Aadhar number. Must be 12 digits."}, 400
            
            # Validate PAN
            if not re.match(r'^[A-Z]{5}\d{4}[A-Z]$', body['panNumber'].upper()):
                return {"error": "Invalid PAN number. Format: ABCDE1234F"}, 400

            aadhar_number = body['aadharNumber']
            pan_number = body['panNumber'].upper()

            # Check if Aadhar or PAN already used by another rider
            existing_aadhar_rider = UserService.get_rider_by_aadhar(aadhar_number)
            if existing_aadhar_rider and existing_aadhar_rider.phone != phone:
                return {"error": "Aadhar number already used by another rider"}, 400

            existing_pan_rider = UserService.get_rider_by_pan(pan_number)
            if existing_pan_rider and existing_pan_rider.phone != phone:
                return {"error": "PAN number already used by another rider"}, 400
            
            # Generate rider ID
            rider_id = generate_id('RDR')
            
            # 1. Create entry in Users table (authentication & KYC)
            user = User(
                phone=phone,
                role="RIDER",
                rider_id=rider_id,
                first_name=body['firstName'],
                last_name=body['lastName'],
                address=body['address'],
                aadhar_number=aadhar_number,
                aadhar_image_url=body['aadharImageUrl'],  # S3 URL instead of base64
                pan_number=pan_number,
                pan_image_url=body['panImageUrl'],  # S3 URL instead of base64
                rider_status=User.RIDER_STATUS_SIGNUP_DONE,
                is_active=False
            )
            
            created_user = UserService.create_user(user)
            logger.info(f"Created user record for rider: {phone}")
            
            # 2. Create entry in Riders table (operational data)
            rider = Rider(
                rider_id=rider_id,
                phone=phone,
                is_active=False,
                lat=None,
                lng=None,
                speed=0.0,
                heading=0.0,
                working_on_order=None
            )
            
            created_rider = RiderService.create_rider(rider)
            logger.info(f"Created rider operational record: {rider_id}")
            
            metrics.add_metric(name="RiderSignupSubmitted", unit="Count", value=1)
            
            return {
                "phone": created_user.phone,
                "riderId": rider_id,
                "status": "SIGNUP_DONE",
                "message": "Your application is under review. We'll notify you once approved (usually within 24-48 hours)."
            }, 201
            
        except Exception as e:
            logger.error("Error in rider signup", exc_info=True)
            metrics.add_metric(name="RiderSignupFailed", unit="Count", value=1)
            return {"error": "Failed to submit signup", "message": str(e)}, 500

    @app.get("/api/v1/riders/<rider_id>/documents")
    @tracer.capture_method
    def get_rider_documents(rider_id: str):
        """
        Fetch rider's Aadhar and PAN document images as base64 from S3.
        """
        try:
            if not rider_id:
                return {"error": "riderId is required"}, 400

            user = UserService.get_rider_by_rider_id(rider_id)
            if not user:
                return {"error": "Rider not found"}, 404

            def _extract_key_parts(url: str):
                if not url:
                    return None, None
                parts = url.split('/')
                if len(parts) < 2:
                    return None, None
                return parts[-2], parts[-1]

            def _get_base64_for_url(url: str):
                mobile, filename = _extract_key_parts(url)
                if not mobile or not filename:
                    return None
                key = f"riders/{mobile}/{filename}"
                s3_object = s3_client.get_object(
                    Bucket=RIDER_DOCUMENTS_BUCKET,
                    Key=key
                )
                image_bytes = s3_object["Body"].read()
                return base64.b64encode(image_bytes).decode("utf-8")

            aadhar_base64 = _get_base64_for_url(user.aadhar_image_url)
            pan_base64 = _get_base64_for_url(user.pan_image_url)

            response = user.to_dict()
            response.update({
                "aadharImageBase64": aadhar_base64,
                "panImageBase64": pan_base64
            })

            metrics.add_metric(name="RiderDocumentsFetched", unit="Count", value=1)
            return response, 200
        except Exception as e:
            logger.error("Error fetching rider documents", exc_info=True)
            metrics.add_metric(name="RiderDocumentsFetchFailed", unit="Count", value=1)
            return {"error": "Failed to fetch rider documents", "message": str(e)}, 500
    
    @app.post("/api/v1/riders/login/check")
    @tracer.capture_method
    def check_rider_login():
        """
        Check if rider can login based on verification status
        
        Request: { "phone": "9876543210" }
        
        Response cases:
        1. Not found: { "status": "NOT_FOUND", "canLogin": false }
        2. Pending: { "status": "SIGNUP_DONE", "canLogin": false, "message": "..." }
        3. Rejected: { "status": "REJECTED", "canLogin": false, "reason": "..." }
        4. Approved: { "status": "APPROVED", "canLogin": true, "riderId": "..." }
        """
        try:
            body = app.current_event.json_body
            phone = body.get('phone')
            
            if not phone:
                return {"error": "Phone number required"}, 400
            
            # Query Users table for RIDER authentication status
            user = UserService.get_user_by_role(phone, "RIDER")
            
            if not user:
                return {
                    "status": "NOT_FOUND",
                    "canLogin": False,
                    "message": "Please signup first to become a delivery partner"
                }, 200
            
            if user.rider_status == User.RIDER_STATUS_SIGNUP_DONE:
                return {
                    "status": "SIGNUP_DONE",
                    "canLogin": False,
                    "message": "Your application is under verification. We'll notify you once approved."
                }, 200
            
            if user.rider_status == User.RIDER_STATUS_REJECTED:
                return {
                    "status": "REJECTED",
                    "canLogin": False,
                    "message": f"Application rejected: {user.rejection_reason or 'Please contact support'}",
                    "reason": user.rejection_reason
                }, 200
            
            if user.rider_status == User.RIDER_STATUS_APPROVED:
                return {
                    "status": "APPROVED",
                    "canLogin": True,
                    "phone": user.phone,
                    "riderId": user.rider_id,
                    "name": f"{user.first_name} {user.last_name}"
                }, 200
            
            return {"error": "Invalid rider status"}, 500
            
        except Exception as e:
            logger.error("Error checking rider login", exc_info=True)
            return {"error": "Failed to check login status", "message": str(e)}, 500
    
    @app.get("/api/v1/riders/list")
    @tracer.capture_method
    def list_riders():
        """
        List riders by status
        Query Parameters:
        - status: SIGNUP_DONE (default), APPROVED, or REJECTED
        
        Response:
        {
            "riders": [
                {
                    "phone": "+919876543210",
                    "riderId": "RDR123",
                    "firstName": "John",
                    "lastName": "Doe",
                    "email": "john@example.com",
                    "address": "123 Street",
                    "aadharNumber": "123456789012",
                    "aadharImageUrl": "s3://...",
                    "panNumber": "ABCDE1234F",
                    "panImageUrl": "s3://...",
                    "riderStatus": "SIGNUP_DONE",
                    "createdAt": "2024-01-01T00:00:00",
                    "isActive": false
                }
            ],
            "count": 1
        }
        """
        try:
            # Get status from query parameters, default to SIGNUP_DONE (pending approval)
            query_params = app.current_event.query_string_parameters or {}
            status = query_params.get('status', User.RIDER_STATUS_SIGNUP_DONE)
            
            # Validate status
            valid_statuses = [
                User.RIDER_STATUS_SIGNUP_DONE,
                User.RIDER_STATUS_APPROVED,
                User.RIDER_STATUS_REJECTED
            ]
            if status not in valid_statuses:
                return {
                    "error": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
                }, 400
            
            # Fetch riders from database
            riders = UserService.list_riders_by_status(status)
            
            # Convert to dictionary format
            riders_data = [rider.to_dict() for rider in riders]
            
            logger.info(f"Listed {len(riders_data)} riders with status: {status}")
            
            return {
                "riders": riders_data,
                "count": len(riders_data),
                "status": status
            }, 200
            
        except Exception as e:
            logger.error("Error listing riders", exc_info=True)
            return {"error": "Failed to list riders", "message": str(e)}, 500
    
    @app.put("/api/v1/riders/approve")
    @tracer.capture_method
    def approve_rider():
        """Approve rider (ops team) - Updates both Users and Riders tables"""
        try:
            body = app.current_event.json_body
            phone = body.get('phone')
            
            if not phone:
                return {"error": "Phone number required"}, 400
            
            user = UserService.get_user_by_role(phone, "RIDER")
            if not user:
                return {"error": "Rider not found"}, 404
            
            # 1. Update Users table (RIDER role)
            UserService.update_user(phone, "RIDER", {
                'riderStatus': User.RIDER_STATUS_APPROVED,
                'isActive': True,
                'approvedAt': datetime.utcnow().isoformat()
            })
            
            # 2. Enable in Riders table (ready for order assignment, but offline by default)
            rider = RiderService.get_rider_by_mobile(phone)
            if rider:
                RiderService.set_active_status(rider.rider_id, False)
            
            logger.info(f"Rider approved: {phone}")
            metrics.add_metric(name="RiderApproved", unit="Count", value=1)
            
            return {"message": "Rider approved successfully"}, 200
            
        except Exception as e:
            logger.error("Error approving rider", exc_info=True)
            return {"error": "Failed to approve rider", "message": str(e)}, 500
    
    @app.put("/api/v1/riders/reject")
    @tracer.capture_method
    def reject_rider():
        """Reject rider (ops team)"""
        try:
            body = app.current_event.json_body
            phone = body.get('phone')
            reason = body.get('reason', 'Document verification failed')
            
            if not phone:
                return {"error": "Phone number required"}, 400
            
            user = UserService.get_user_by_role(phone, "RIDER")
            if not user:
                return {"error": "Rider not found"}, 404
            
            # Update Users table (RIDER role)
            UserService.update_user(phone, "RIDER", {
                'riderStatus': User.RIDER_STATUS_REJECTED,
                'rejectionReason': reason,
                'isActive': False
            })
            
            logger.info(f"Rider rejected: {phone} - {reason}")
            metrics.add_metric(name="RiderRejected", unit="Count", value=1)
            
            return {"message": "Rider rejected"}, 200
            
        except Exception as e:
            logger.error("Error rejecting rider", exc_info=True)
            return {"error": "Failed to reject rider", "message": str(e)}, 500
