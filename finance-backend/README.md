# Finance Data Processing and Access Control Backend

A backend API for managing users, financial records, role-based permissions, and dashboard summaries.

## Tech Stack
- Node.js
- Express
- MongoDB
- Mongoose
- JWT Authentication

## Features
- User and role management
- Active/inactive user status
- Financial record CRUD
- Record filtering by type, category, date, and search
- Dashboard summary APIs
- Role-based access control
- Validation and error handling
- Soft delete for records

## Roles
- Viewer: can view dashboard data
- Analyst: can view records and dashboard summaries
- Admin: can manage users and records

## Setup
1. Install dependencies
   `npm install`

2. Create a `.env` file
   `PORT=5000`
   `MONGO_URI=your_mongo_uri`
   `JWT_SECRET=your_secret`
   `JWT_EXPIRES_IN=7d`

3. Start the server
   `npm run dev`

## Assumptions
- Public registration is allowed only for the first user, which becomes admin.
- After the first user is created, all new users must be created by an admin.
- Records are soft deleted using `isDeleted`.
- Inactive users cannot log in or access protected routes.

## Main Endpoints
- POST `/api/auth/register`
- POST `/api/auth/login`
- GET `/api/dashboard`
- GET `/api/records`
- POST `/api/records`
- GET `/api/users`
- POST `/api/users`
