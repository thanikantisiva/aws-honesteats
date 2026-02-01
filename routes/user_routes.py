"""User routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.user_service import UserService
from models.user import User

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_user_routes(app):
    """Register user routes"""
    
    @app.get("/api/v1/users/<phone>")
    @tracer.capture_method
    def get_user(phone: str):
        """Get user by phone number (CUSTOMER role)"""
        try:
            logger.info(f"Getting user: {phone}")
            user = UserService.get_user_by_role(phone, "CUSTOMER")
            
            if not user:
                return {"error": "User not found"}, 404
            
            metrics.add_metric(name="UserRetrieved", unit="Count", value=1)
            return user.to_dict(), 200
        except Exception as e:
            logger.error("Error getting user", exc_info=True)
            return {"error": "Failed to get user", "message": str(e)}, 500
    
    @app.post("/api/v1/users")
    @tracer.capture_method
    def create_user():
        """Create a new user"""
        try:
            body = app.current_event.json_body
            phone = body.get('phone')
            name = body.get('name', 'User')
            email = body.get('email')
            date_of_birth = body.get('dateOfBirth')
            
            if not phone:
                return {"error": "Phone number is required"}, 400
            
            logger.info(f"Creating user: {phone}")
            
            # Check if CUSTOMER role already exists (RIDER role is OK)
            existing_customer = UserService.get_user_by_role(phone, "CUSTOMER")
            if existing_customer:
                return {"error": "User already exists"}, 409
            
            user = User(
                phone=phone,
                name=name,
                email=email,
                date_of_birth=date_of_birth
            )
            
            created_user = UserService.create_user(user)
            metrics.add_metric(name="UserCreated", unit="Count", value=1)
            
            return created_user.to_dict(), 201
        except Exception as e:
            logger.error("Error creating user", exc_info=True)
            return {"error": "Failed to create user", "message": str(e)}, 500
    
    @app.put("/api/v1/users/<phone>")
    @tracer.capture_method
    def update_user(phone: str):
        """Update user information"""
        try:
            body = app.current_event.json_body
            updates = {}
            
            if 'name' in body:
                updates['name'] = body['name']
            if 'email' in body:
                updates['email'] = body['email']
            if 'isActive' in body:
                updates['isActive'] = body['isActive']
            if 'dateOfBirth' in body:
                updates['dateOfBirth'] = body['dateOfBirth']
            
            if not updates:
                return {"error": "No fields to update"}, 400
            
            logger.info(f"Updating user: {phone}, updates: {updates}")
            
            updated_user = UserService.update_user(phone, "CUSTOMER", updates)
            metrics.add_metric(name="UserUpdated", unit="Count", value=1)
            
            return updated_user.to_dict(), 200
        except Exception as e:
            logger.error("Error updating user", exc_info=True)
            return {"error": "Failed to update user", "message": str(e)}, 500
    
    @app.post("/api/v1/users/<phone>/fcm-token")
    @tracer.capture_method
    def register_fcm_token(phone: str):
        """Register or update FCM token for push notifications - creates user if not exists"""
        try:
            body = app.current_event.json_body
            fcm_token = body.get('fcmToken')
            
            if not fcm_token:
                return {"error": "fcmToken is required"}, 400
            
            logger.info(f"📱 Registering FCM token for: {phone[:5]}***")
            logger.info(f"🔑 Token: {fcm_token[:30]}...")
            
            from datetime import datetime
            
            # Get all roles for this phone and update FCM token for all
            all_roles = UserService.get_all_user_roles(phone)
            
            if all_roles:
                # Update FCM token for all existing roles
                logger.info(f"✅ Found {len(all_roles)} role(s), updating FCM token for all")
                for user_role in all_roles:
                    UserService.update_user(phone, user_role.role, {
                        'fcmToken': fcm_token,
                        'fcmTokenUpdatedAt': datetime.utcnow().isoformat()
                    })
            else:
                # User doesn't exist - create CUSTOMER with FCM token
                logger.info(f"🆕 User not found, creating CUSTOMER with FCM token")
                from models.user import User
                new_user = User(
                    phone=phone,
                    name='',  # Will be updated during registration
                    email='',
                    role='CUSTOMER',
                    is_active=True,
                    fcm_token=fcm_token,
                    fcm_token_updated_at=datetime.utcnow().isoformat()
                )
                UserService.create_user(new_user)
                logger.info(f"✅ User created with FCM token")
            
            metrics.add_metric(name="FCMTokenRegistered", unit="Count", value=1)
            
            return {"message": "FCM token registered successfully"}, 200
        except Exception as e:
            logger.error(f"❌ Error registering FCM token: {str(e)}", exc_info=True)
            return {"error": "Failed to register FCM token", "message": str(e)}, 500

