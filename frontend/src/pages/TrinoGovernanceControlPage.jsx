import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  Chip,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
} from '@mui/material';
import {
  Refresh,
  Security,
  TableChart,
  AutoAwesome,
  Folder,
  Schema,
  ArrowBack,
  ArrowForward,
  AccountTree,
} from '@mui/icons-material';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import ReactFlow, { Background, Controls, MiniMap } from 'reactflow';
import 'reactflow/dist/style.css';

const TrinoGovernanceControlPage = () => {
  const [governanceData, setGovernanceData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedRole, setSelectedRole] = useState(null);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [selectedUser, setSelectedUser] = useState(null);
  const [viewMode, setViewMode] = useState('overview'); // overview, roles, assets, users
  const [rolesPage, setRolesPage] = useState(0);
  const [assetsPage, setAssetsPage] = useState(0);
  const [assetsPageSize] = useState(50); // Assets per page
  const [distributionDialogOpen, setDistributionDialogOpen] = useState(false);
  const [rolesStatusDialogOpen, setRolesStatusDialogOpen] = useState(false);

  useEffect(() => {
    loadGovernanceData(0);
  }, []);

  const loadGovernanceData = async (page = 0) => {
    setLoading(true);
    setError(null);
    try {
      const url = `http://localhost:8099/api/starburst/governance-control?assets_page=${page}&assets_page_size=${assetsPageSize}`;
      const response = await fetch(url);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || 'Failed to fetch governance data');
      }
      const data = await response.json();
      setGovernanceData(data);
      setAssetsPage(page); // Update current page
    } catch (err) {
      setError(err.message);
      console.error('Error loading governance data:', err);
    } finally {
      setLoading(false);
    }
  };

  const renderRoleCard = (role) => (
    <Card 
      key={role.role_name}
      variant="outlined"
      sx={{ 
        cursor: 'pointer',
        transition: 'all 0.2s',
        height: '100%',
        '&:hover': {
          boxShadow: 3,
          transform: 'translateY(-2px)'
        }
      }}
      onClick={() => setSelectedRole(role)}
    >
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <Security sx={{ mr: 1, color: '#1976d2' }} />
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            {role.role_name}
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
          <Chip 
            label={`${role.users.length} Users`}
            size="small"
            color="primary"
            variant="outlined"
            sx={{ fontWeight: 600 }}
          />
          <Chip 
            label={`${typeof role._total_asset_count === 'number' ? role._total_asset_count : role.asset_permissions.length} Assets`}
            size="small"
            color="secondary"
            variant="filled"
            sx={{ fontWeight: 600, bgcolor: '#9c27b0' }}
          />
        </Box>
        {role.users.length > 0 && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Users: {role.users.slice(0, 3).join(', ')}
              {role.users.length > 3 && ` +${role.users.length - 3} more`}
            </Typography>
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderAssetCard = (asset) => (
    <Card 
      key={`${asset.catalog}.${asset.schema}.${asset.table}`}
      variant="outlined"
      sx={{ 
        cursor: 'pointer',
        transition: 'all 0.2s',
        height: '100%',
        '&:hover': {
          boxShadow: 3,
          transform: 'translateY(-2px)'
        }
      }}
      onClick={() => setSelectedAsset(asset)}
    >
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <TableChart sx={{ mr: 1, color: '#00D4AA' }} />
          <Typography variant="h6" sx={{ fontWeight: 600, fontSize: '1rem' }}>
            {asset.table}
          </Typography>
        </Box>
        <Box sx={{ mb: 2 }}>
          <Typography variant="caption" color="text.secondary" display="block">
            <Folder sx={{ fontSize: 12, mr: 0.5, verticalAlign: 'middle' }} />
            {asset.catalog}
          </Typography>
          <Typography variant="caption" color="text.secondary" display="block">
            <Schema sx={{ fontSize: 12, mr: 0.5, verticalAlign: 'middle' }} />
            {asset.schema}
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          {asset.privilege_type.map((priv, idx) => (
            <Chip 
              key={idx}
              label={priv}
              size="small"
              color="success"
              variant="outlined"
              sx={{ fontSize: '0.7rem' }}
            />
          ))}
          <Chip 
            label={`${asset.roles_with_access.length} Roles`}
            size="small"
            color="primary"
            variant="filled"
            sx={{ fontSize: '0.7rem' }}
          />
        </Box>
      </CardContent>
    </Card>
  );

  const renderUserCard = (user) => (
    <Card 
      key={user.username}
      variant="outlined"
      sx={{ 
        mb: 2,
        cursor: 'pointer',
        transition: 'all 0.2s',
        '&:hover': {
          boxShadow: 3,
          transform: 'translateY(-2px)'
        }
      }}
      onClick={() => setSelectedUser(user)}
    >
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 600, fontSize: '1rem' }}>
              {user.username}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {user.roles.length} {user.roles.length === 1 ? 'role' : 'roles'} assigned
            </Typography>
          </Box>
          <Chip 
            label="Active"
            size="small"
            color="success"
            variant="outlined"
          />
        </Box>
        <Box sx={{ mt: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          {user.roles.map((role, idx) => (
            <Chip 
              key={idx}
              label={role}
              size="small"
              color="primary"
              variant="outlined"
            />
          ))}
        </Box>
      </CardContent>
    </Card>
  );

  const COLORS = ['#1976d2', '#00D4AA', '#f57c00', '#9c27b0', '#4caf50', '#ff9800'];

  const renderOverview = () => {
    // Prepare pie chart data
    const pieData = [
      { name: 'Roles', value: governanceData.total_roles, color: '#1976d2' },
      { name: 'Users', value: governanceData.total_users, color: '#00D4AA' },
      { name: 'Protected Assets', value: governanceData.total_assets_with_rbac, color: '#f57c00' }
    ];

    // Get role distribution by user count
    const roleUserDistribution = governanceData.roles.map(role => ({
      name: role.role_name,
      users: role.users.length,
      assets: typeof role._total_asset_count === 'number' ? role._total_asset_count : role.asset_permissions.length
    })).filter(role => role.users > 0).slice(0, 6);

    // Calculate insights
    const rolesWithUsers = governanceData.roles.filter(r => r.users.length > 0).length;
    const rolesWithoutUsers = governanceData.roles.length - rolesWithUsers;
    const avgUsersPerRole = rolesWithUsers > 0 ? (governanceData.total_users / rolesWithUsers).toFixed(1) : 0;
    const schemasWithAssets = [...new Set(governanceData.assets_permissions.map(a => a.schema))].length;
    
    return (
      <Grid container spacing={3}>
        {/* Key Metrics Cards */}
        <Grid item xs={12} md={3}>
          <Card sx={{ bgcolor: '#1976d2', color: 'white', height: '200px', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
              <Typography variant="h3" sx={{ fontWeight: 700, mb: 1, lineHeight: 1 }}>
                {governanceData.total_roles}
              </Typography>
              <Box>
                <Typography variant="h6" sx={{ mb: 0.5 }}>Total Roles</Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }}>
                  {rolesWithUsers} with users
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ bgcolor: '#00D4AA', color: 'white', height: '200px', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
              <Typography variant="h3" sx={{ fontWeight: 700, mb: 1, lineHeight: 1 }}>
                {governanceData.total_users}
              </Typography>
              <Box>
                <Typography variant="h6" sx={{ mb: 0.5 }}>Total Users</Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }}>
                  Avg {avgUsersPerRole} per role
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ bgcolor: '#f57c00', color: 'white', height: '200px', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
              <Typography variant="h3" sx={{ fontWeight: 700, mb: 1, lineHeight: 1 }}>
                {governanceData.total_assets_with_rbac}
              </Typography>
              <Box>
                <Typography variant="h6" sx={{ mb: 0.5 }}>Protected Assets</Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }}>
                  {schemasWithAssets} schemas covered
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ bgcolor: '#9c27b0', color: 'white', height: '200px', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
              <Typography variant="h3" sx={{ fontWeight: 700, mb: 1, lineHeight: 1 }}>
                {rolesWithoutUsers}
              </Typography>
              <Box>
                <Typography variant="h6" sx={{ mb: 0.5 }}>Orphaned Roles</Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }}>
                  No user assignments
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Governance Insights Stats */}
        <Grid item xs={12}>
          <Card sx={{ bgcolor: '#f8f9fa', border: '1px solid #e0e0e0' }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                📈 Governance Insights
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6} md={3}>
                  <Box sx={{ p: 2, bgcolor: 'white', borderRadius: 2, textAlign: 'center' }}>
                    <Typography variant="h4" sx={{ color: '#1976d2', fontWeight: 700, mb: 1 }}>
                      {((rolesWithUsers / governanceData.total_roles) * 100).toFixed(0)}%
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Roles with assigned users
                    </Typography>
                  </Box>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Box sx={{ p: 2, bgcolor: 'white', borderRadius: 2, textAlign: 'center' }}>
                    <Typography variant="h4" sx={{ color: '#00D4AA', fontWeight: 700, mb: 1 }}>
                      {((governanceData.total_users / rolesWithUsers).toFixed(1))}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Avg users per active role
                    </Typography>
                  </Box>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Box sx={{ p: 2, bgcolor: 'white', borderRadius: 2, textAlign: 'center' }}>
                    <Typography variant="h4" sx={{ color: '#f57c00', fontWeight: 700, mb: 1 }}>
                      {schemasWithAssets}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Schemas under RBAC protection
                    </Typography>
                  </Box>
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                  <Box sx={{ p: 2, bgcolor: 'white', borderRadius: 2, textAlign: 'center' }}>
                    <Typography variant="h4" sx={{ color: '#9c27b0', fontWeight: 700, mb: 1 }}>
                      {rolesWithoutUsers}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Orphaned roles (no users)
                    </Typography>
                  </Box>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        {/* Distribution Chart */}
        <Grid item xs={12} md={6}>
          <Card 
            sx={{ 
              cursor: 'pointer',
              transition: 'all 0.2s',
              '&:hover': {
                boxShadow: 4,
                transform: 'translateY(-2px)'
              }
            }}
            onClick={() => setDistributionDialogOpen(true)}
          >
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                📊 Distribution Overview
              </Typography>
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '275px' }}>
                <ResponsiveContainer width="100%" height={250}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: 'Roles', value: governanceData.total_roles, color: '#1976d2' },
                        { name: 'Users', value: governanceData.total_users, color: '#00D4AA' },
                        { name: 'Protected Assets', value: governanceData.total_assets_with_rbac, color: '#f57c00' }
                      ]}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {[{ name: 'Roles', value: governanceData.total_roles, color: '#1976d2' },
                        { name: 'Users', value: governanceData.total_users, color: '#00D4AA' },
                        { name: 'Protected Assets', value: governanceData.total_assets_with_rbac, color: '#f57c00' }].map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Active vs Orphaned Roles */}
        <Grid item xs={12} md={6}>
          <Card
            sx={{ 
              cursor: 'pointer',
              transition: 'all 0.2s',
              '&:hover': {
                boxShadow: 4,
                transform: 'translateY(-2px)'
              }
            }}
            onClick={() => setRolesStatusDialogOpen(true)}
          >
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                🔐 Roles Status Breakdown
              </Typography>
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '275px' }}>
                <ResponsiveContainer width="100%" height={250}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: 'Active Roles', value: rolesWithUsers, color: '#4caf50' },
                        { name: 'Orphaned Roles', value: rolesWithoutUsers, color: '#ff9800' }
                      ]}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, value, percent }) => `${name}: ${value} (${(percent * 100).toFixed(0)}%)`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {[
                        { name: 'Active Roles', value: rolesWithUsers, color: '#4caf50' },
                        { name: 'Orphaned Roles', value: rolesWithoutUsers, color: '#ff9800' }
                      ].map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Roles Overview Table */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                📊 Roles Overview
              </Typography>
              <TableContainer component={Paper} variant="outlined">
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600 }}>Role Name</TableCell>
                      <TableCell sx={{ fontWeight: 600 }} align="right">Users</TableCell>
                      <TableCell sx={{ fontWeight: 600 }} align="right">Assets Access</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {governanceData.roles.slice(rolesPage * 3, rolesPage * 3 + 3).map((role) => (
                      <TableRow 
                        key={role.role_name}
                        hover
                        sx={{ cursor: 'pointer' }}
                        onClick={() => setSelectedRole(role)}
                      >
                        <TableCell>
                          <Box sx={{ display: 'flex', alignItems: 'center' }}>
                            <Security sx={{ mr: 1, fontSize: 20, color: '#1976d2' }} />
                            <strong>{role.role_name}</strong>
                          </Box>
                        </TableCell>
                        <TableCell align="right">
                          <Chip label={role.users.length} color="primary" size="small" />
                        </TableCell>
                        <TableCell align="right">
                          <Chip label={typeof role._total_asset_count === 'number' ? role._total_asset_count : role.asset_permissions.length} color="secondary" size="small" />
                        </TableCell>
                        <TableCell>
                          <Chip 
                            label={role.users.length > 0 ? 'Active' : 'Orphaned'} 
                            color={role.users.length > 0 ? 'success' : 'warning'} 
                            size="small" 
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
                <Typography variant="body2" color="text.secondary">
                  Showing {rolesPage * 3 + 1}-{Math.min(rolesPage * 3 + 3, governanceData.roles.length)} of {governanceData.roles.length} roles
                </Typography>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    size="small"
                    startIcon={<ArrowBack />}
                    onClick={() => setRolesPage(p => Math.max(0, p - 1))}
                    disabled={rolesPage === 0}
                  >
                    Previous
                  </Button>
                  <Button
                    size="small"
                    endIcon={<ArrowForward />}
                    onClick={() => setRolesPage(p => Math.min(Math.ceil(governanceData.roles.length / 3) - 1, p + 1))}
                    disabled={(rolesPage + 1) * 3 >= governanceData.roles.length}
                  >
                    Next
                  </Button>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Top Assets by Access Count */}
        <Grid item xs={12} md={4}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                🔝 Most Accessible Assets
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                {[...governanceData.assets_permissions]
                  .sort((a, b) => b.roles_with_access.length - a.roles_with_access.length)
                  .slice(assetsPage * 2, assetsPage * 2 + 2)
                  .map((asset, idx) => (
                    <Box key={idx} sx={{ p: 2, bgcolor: '#f5f5f5', borderRadius: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600 }} noWrap>
                        {asset.table}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {asset.catalog}.{asset.schema}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                        <Chip label={`${asset.roles_with_access.length} roles`} size="small" />
                        {asset.privilege_type.map((priv, pidx) => (
                          <Chip key={pidx} label={priv} size="small" color="success" variant="outlined" />
                        ))}
                      </Box>
                    </Box>
                  ))}
              </Box>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
                <Typography variant="body2" color="text.secondary">
                  Showing {assetsPage * 2 + 1}-{Math.min(assetsPage * 2 + 2, governanceData.assets_permissions.length)} of {governanceData.assets_permissions.length} assets
                </Typography>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    size="small"
                    startIcon={<ArrowBack />}
                    onClick={() => setAssetsPage(p => Math.max(0, p - 1))}
                    disabled={assetsPage === 0}
                  >
                    Previous
                  </Button>
                  <Button
                    size="small"
                    endIcon={<ArrowForward />}
                    onClick={() => setAssetsPage(p => Math.min(Math.ceil(governanceData.assets_permissions.length / 2) - 1, p + 1))}
                    disabled={(assetsPage + 1) * 2 >= governanceData.assets_permissions.length}
                  >
                    Next
                  </Button>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* User-to-Role Mapping */}
        <Grid item xs={12}>
          <Card sx={{ width: '100%' }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
                👥 User-to-Role Assignments
              </Typography>
              <Grid container spacing={2}>
                {governanceData.users.slice(0, 10).map((user) => (
                  <Grid item xs={12} key={user.username}>
                    {renderUserCard(user)}
                  </Grid>
                ))}
              </Grid>
              {governanceData.users.length > 10 && (
                <Typography variant="body2" color="text.secondary" sx={{ mt: 2, textAlign: 'center' }}>
                  Showing 10 of {governanceData.users.length} users
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    );
  };

  const renderRolesView = () => (
    <Grid container spacing={3}>
      {governanceData.roles.map((role) => (
        <Grid item xs={12} sm={6} md={4} key={role.role_name}>
          {renderRoleCard(role)}
        </Grid>
      ))}
    </Grid>
  );

  const renderAssetsView = () => {
    const assetsPagination = governanceData.assets_pagination || {};
    const totalAssets = assetsPagination.total || governanceData.total_assets_with_rbac || 0;
    const currentPage = assetsPagination.page || 0;
    const totalPages = assetsPagination.total_pages || 1;
    const hasNext = assetsPagination.has_next || false;
    const hasPrev = assetsPagination.has_prev || false;
    
    return (
      <>
        <Grid container spacing={3}>
          {governanceData.assets_permissions.map((asset, idx) => (
            <Grid item xs={12} sm={6} md={4} key={idx}>
              {renderAssetCard(asset)}
            </Grid>
          ))}
        </Grid>
        
        {/* Pagination Controls */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 4, mb: 2 }}>
          <Typography variant="body2" color="text.secondary">
            Showing {currentPage * assetsPageSize + 1}-{Math.min((currentPage + 1) * assetsPageSize, totalAssets)} of {totalAssets} assets
          </Typography>
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Button
              size="small"
              startIcon={<ArrowBack />}
              onClick={() => loadGovernanceData(currentPage - 1)}
              disabled={!hasPrev || loading}
            >
              Previous
            </Button>
            <Typography variant="body2" sx={{ display: 'flex', alignItems: 'center', px: 2 }}>
              Page {currentPage + 1} of {totalPages}
            </Typography>
            <Button
              size="small"
              endIcon={<ArrowForward />}
              onClick={() => loadGovernanceData(currentPage + 1)}
              disabled={!hasNext || loading}
            >
              Next
            </Button>
          </Box>
        </Box>
      </>
    );
  };

  const renderUsersView = () => (
    <Grid container spacing={3}>
      <Grid item xs={12}>
        {governanceData.users.map((user) => renderUserCard(user))}
      </Grid>
    </Grid>
  );

  const renderSpiderMapView = () => {
    const flowNodes = [];
    const flowEdges = [];

    // Helper function to distribute nodes in a circle
    const circularLayout = (count, radius, centerX, centerY) => {
      const positions = [];
      for (let i = 0; i < count; i++) {
        const angle = (2 * Math.PI * i) / count;
        positions.push({
          x: centerX + radius * Math.cos(angle),
          y: centerY + radius * Math.sin(angle)
        });
      }
      return positions;
    };

    // Get more data - showing up to 10 of each type
    const rolesLimited = governanceData.roles.slice(0, 10);
    const usersLimited = governanceData.users.slice(0, 10);
    const assetsLimited = [...governanceData.assets_permissions]
      .sort((a, b) => b.roles_with_access.length - a.roles_with_access.length)
      .slice(0, 12);

    // Calculate positions with better spacing
    const rolePositions = circularLayout(rolesLimited.length, 150, 500, 400);
    const userPositions = circularLayout(usersLimited.length, 280, 500, 400);
    const assetPositions = circularLayout(assetsLimited.length, 420, 500, 400);

    // Create nodes for roles (limited to first 15)
    rolesLimited.forEach((role, idx) => {
      flowNodes.push({
        id: `role-${role.role_name}`,
        type: 'default',
        position: rolePositions[idx],
        data: { 
          label: role.role_name,
          type: 'role'
        },
        style: { 
          background: '#1976d2', 
          color: '#fff',
          border: '2px solid #0d47a1',
          borderRadius: '8px',
          padding: '10px',
          fontWeight: 600,
          width: 120
        }
      });
    });

    // Create nodes for users (limited to first 15)
    usersLimited.forEach((user, idx) => {
      flowNodes.push({
        id: `user-${user.username}`,
        type: 'default',
        position: userPositions[idx],
        data: { 
          label: user.username,
          type: 'user'
        },
        style: { 
          background: '#00D4AA', 
          color: '#fff',
          border: '2px solid #00a88a',
          borderRadius: '8px',
          padding: '10px',
          fontWeight: 600,
          width: 120
        }
      });
    });

    // Create nodes for top assets (limited to first 10)
    assetsLimited.forEach((asset, idx) => {
      flowNodes.push({
        id: `asset-${asset.catalog}-${asset.schema}-${asset.table}`,
        type: 'default',
        position: assetPositions[idx],
        data: { 
          label: asset.table,
          type: 'asset'
        },
        style: { 
          background: '#f57c00', 
          color: '#fff',
          border: '2px solid #e65100',
          borderRadius: '8px',
          padding: '10px',
          fontWeight: 600,
          width: 120
        }
      });
    });

    // Create asset map for easy lookup
    const assetMap = new Map();
    assetsLimited.forEach((asset) => {
      assetMap.set(`${asset.catalog}.${asset.schema}.${asset.table}`, 
        `asset-${asset.catalog}-${asset.schema}-${asset.table}`);
    });

    // Create role map for easy lookup
    const roleMap = new Map();
    rolesLimited.forEach((role) => {
      roleMap.set(role.role_name, true);
    });

    // Create edges from users to roles (limit to 2-3 connections max per user to reduce congestion)
    usersLimited.forEach((user) => {
      const userRoles = user.roles.filter(r => roleMap.has(r)).slice(0, 3); // Max 3 connections per user
      userRoles.forEach((roleName) => {
        flowEdges.push({
          id: `user-${user.username}-role-${roleName}`,
          source: `user-${user.username}`,
          target: `role-${roleName}`,
          type: 'straight', // Use straight lines instead of curved to reduce visual clutter
          animated: false, // Disable animation to reduce visual noise
          style: { stroke: '#00D4AA', strokeWidth: 1.5 },
          markerEnd: { type: 'arrowclosed' }
        });
      });
    });

    // Create edges from roles to assets (limit to 2-3 most important assets per role)
    rolesLimited.forEach((role) => {
      const roleAssets = [];
      role.asset_permissions.forEach((permission) => {
        const assetKey = `${permission.catalog}.${permission.schema}.${permission.table}`;
        if (assetMap.has(assetKey)) {
          const fullAssetKey = assetMap.get(assetKey);
          if (!roleAssets.includes(fullAssetKey)) {
            roleAssets.push(fullAssetKey);
          }
        }
      });
      
      // Only connect to top 3 assets per role
      roleAssets.slice(0, 3).forEach((fullAssetKey) => {
        flowEdges.push({
          id: `role-${role.role_name}-asset-${fullAssetKey}`,
          source: `role-${role.role_name}`,
          target: fullAssetKey,
          type: 'straight', // Use straight lines
          animated: false,
          style: { stroke: '#1976d2', strokeWidth: 1.5 },
          markerEnd: { type: 'arrowclosed' }
        });
      });
    });

    return (
      <Box sx={{ p: 3 }}>
        <Typography variant="h5" sx={{ mb: 3, fontWeight: 600 }}>
          🕸️ Governance Spider Map
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
          Interactive visualization of roles, users, and assets relationships
          <br />
          <strong>Showing:</strong> {rolesLimited.length} roles, {usersLimited.length} users, {assetsLimited.length} assets
          <br />
          <em>Limited view to reduce visual congestion. Use "Roles", "Assets", and "Users" tabs for complete data.</em>
        </Typography>
        
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={12} md={4}>
            <Card sx={{ p: 2, bgcolor: '#1976d2', color: 'white' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Security sx={{ fontSize: 30 }} />
                <Box>
                  <Typography variant="h4">{governanceData.roles.length}</Typography>
                  <Typography variant="body2">Roles</Typography>
                </Box>
              </Box>
            </Card>
          </Grid>
          <Grid item xs={12} md={4}>
            <Card sx={{ p: 2, bgcolor: '#00D4AA', color: 'white' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <AutoAwesome sx={{ fontSize: 30 }} />
                <Box>
                  <Typography variant="h4">{governanceData.users.length}</Typography>
                  <Typography variant="body2">Users</Typography>
                </Box>
              </Box>
            </Card>
          </Grid>
          <Grid item xs={12} md={4}>
            <Card sx={{ p: 2, bgcolor: '#f57c00', color: 'white' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <TableChart sx={{ fontSize: 30 }} />
                <Box>
                  <Typography variant="h4">{governanceData.assets_permissions.length}</Typography>
                  <Typography variant="body2">Assets</Typography>
                </Box>
              </Box>
            </Card>
          </Grid>
        </Grid>

        <Card sx={{ height: '800px', width: '100%' }}>
          <ReactFlow
            nodes={flowNodes}
            edges={flowEdges}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            nodesDraggable
            nodesConnectable={false}
            elementsSelectable={true}
            defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
          >
            <Background gap={16} />
            <Controls />
            <MiniMap position="top-right" nodeColor={(node) => {
              const data = node.data || {};
              if (data.type === 'role') return '#1976d2';
              if (data.type === 'user') return '#00D4AA';
              return '#f57c00';
            }} />
          </ReactFlow>
        </Card>

        <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
          <Chip label="Blue = Roles" sx={{ bgcolor: '#1976d2', color: 'white' }} />
          <Chip label="Green = Users" sx={{ bgcolor: '#00D4AA', color: 'white' }} />
          <Chip label="Orange = Assets" sx={{ bgcolor: '#f57c00', color: 'white' }} />
        </Box>
      </Box>
    );
  };

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <Card sx={{ boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)', borderRadius: 2 }}>
          <CardContent sx={{ p: 4, textAlign: 'center' }}>
            <CircularProgress sx={{ mb: 2 }} />
            <Typography variant="h6">Loading Governance Control Data...</Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Fetching roles, users, and asset permissions from Starburst Galaxy
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Card sx={{ boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)', borderRadius: 2 }}>
          <CardContent sx={{ p: 4 }}>
            <Alert severity="error" sx={{ mb: 2 }}>
              <Typography variant="h6" sx={{ mb: 1 }}>Failed to Load Governance Data</Typography>
              {error}
            </Alert>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Please ensure:
            </Typography>
            <ul>
              <li>You have an active Starburst Galaxy connector</li>
              <li>Your Starburst credentials are valid</li>
              <li>The Starburst Galaxy API is accessible</li>
            </ul>
            <Button 
              variant="contained" 
              onClick={loadGovernanceData}
              startIcon={<Refresh />}
              sx={{ mt: 2 }}
            >
              Retry
            </Button>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (!governanceData) {
    return null;
  }

  return (
    <Box sx={{ p: 1 }}>
      <Card sx={{ boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)', borderRadius: 2, maxWidth: '95%', mx: 'auto' }}>
        <CardContent sx={{ p: 6 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography 
              variant="h4" 
              component="h1" 
              sx={{ 
                fontWeight: 600, 
                color: '#1976d2',
              }}
            >
              🔐 Trino Governance Control
            </Typography>
            <Button
              variant="outlined"
              startIcon={<Refresh />}
              onClick={loadGovernanceData}
              disabled={loading}
            >
              Refresh
            </Button>
          </Box>

          {/* View Tabs */}
          <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}>
            <Grid container spacing={1}>
              <Grid item>
                <Button
                  variant={viewMode === 'overview' ? 'contained' : 'text'}
                  onClick={() => setViewMode('overview')}
                  startIcon={<AutoAwesome />}
                  size="large"
                >
                  Overview
                </Button>
              </Grid>
              <Grid item>
                <Button
                  variant={viewMode === 'roles' ? 'contained' : 'text'}
                  onClick={() => setViewMode('roles')}
                  startIcon={<Security />}
                  size="large"
                >
                  Roles ({governanceData.total_roles})
                </Button>
              </Grid>
              <Grid item>
                <Button
                  variant={viewMode === 'assets' ? 'contained' : 'text'}
                  onClick={() => setViewMode('assets')}
                  startIcon={<TableChart />}
                  size="large"
                >
                  Assets ({governanceData.total_assets_with_rbac || 0})
                </Button>
              </Grid>
              <Grid item>
                <Button
                  variant={viewMode === 'users' ? 'contained' : 'text'}
                  onClick={() => setViewMode('users')}
                  size="large"
                >
                  Users ({governanceData.total_users})
                </Button>
              </Grid>
            </Grid>
          </Box>

          {/* Content */}
          {viewMode === 'overview' && renderOverview()}
          {viewMode === 'roles' && renderRolesView()}
          {viewMode === 'assets' && renderAssetsView()}
          {viewMode === 'users' && renderUsersView()}

          {/* Role Details Dialog */}
          <Dialog
            open={selectedRole !== null}
            onClose={() => setSelectedRole(null)}
            maxWidth="md"
            fullWidth
          >
            {selectedRole && (
              <>
                <DialogTitle>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <Security sx={{ mr: 1, color: '#1976d2' }} />
                    Role: {selectedRole.role_name}
                  </Box>
                </DialogTitle>
                <DialogContent>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Users ({selectedRole.users.length})
                  </Typography>
                  <Box sx={{ mb: 3 }}>
                    {selectedRole.users.map((user, idx) => (
                      <Chip key={idx} label={user} sx={{ m: 0.5 }} />
                    ))}
                    {selectedRole.users.length === 0 && (
                      <Typography variant="body2" color="text.secondary">
                        No users assigned to this role
                      </Typography>
                    )}
                  </Box>

                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Asset Permissions ({typeof selectedRole._total_asset_count === 'number' ? selectedRole._total_asset_count : selectedRole.asset_permissions.length})
                  </Typography>
                  <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
                    <Table stickyHeader size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell sx={{ fontWeight: 600 }}>Asset</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Catalog</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Schema</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Privileges</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {selectedRole.asset_permissions.map((perm, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{perm.table}</TableCell>
                            <TableCell>{perm.catalog}</TableCell>
                            <TableCell>{perm.schema}</TableCell>
                            <TableCell>
                              {perm.permissions && perm.permissions.map((priv, pidx) => (
                                <Chip key={pidx} label={priv} size="small" sx={{ m: 0.25 }} />
                              ))}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </DialogContent>
                <DialogActions>
                  <Button onClick={() => setSelectedRole(null)}>Close</Button>
                </DialogActions>
              </>
            )}
          </Dialog>

          {/* Asset Details Dialog */}
          <Dialog
            open={selectedAsset !== null}
            onClose={() => setSelectedAsset(null)}
            maxWidth="sm"
            fullWidth
          >
            {selectedAsset && (
              <>
                <DialogTitle>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <TableChart sx={{ mr: 1, color: '#00D4AA' }} />
                    Asset: {selectedAsset.table}
                  </Box>
                </DialogTitle>
                <DialogContent>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    <strong>Catalog:</strong> {selectedAsset.catalog}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    <strong>Schema:</strong> {selectedAsset.schema}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    <strong>Full Name:</strong> {selectedAsset.full_name || `${selectedAsset.catalog}.${selectedAsset.schema}.${selectedAsset.table}`}
                  </Typography>

                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Privileges
                  </Typography>
                  <Box sx={{ mb: 3 }}>
                    {selectedAsset.privilege_type && selectedAsset.privilege_type.map((priv, idx) => (
                      <Chip key={idx} label={priv} color="success" sx={{ m: 0.5 }} />
                    ))}
                  </Box>

                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Roles with Access ({selectedAsset.roles_with_access ? selectedAsset.roles_with_access.length : 0})
                  </Typography>
                  <Box>
                    {selectedAsset.roles_with_access && selectedAsset.roles_with_access.map((role, idx) => (
                      <Chip key={idx} label={role} color="primary" sx={{ m: 0.5 }} />
                    ))}
                  </Box>
                </DialogContent>
                <DialogActions>
                  <Button onClick={() => setSelectedAsset(null)}>Close</Button>
                </DialogActions>
              </>
            )}
          </Dialog>

          {/* Distribution Overview Dialog */}
          <Dialog
            open={distributionDialogOpen}
            onClose={() => setDistributionDialogOpen(false)}
            maxWidth="md"
            fullWidth
          >
            <DialogTitle>
              <Typography variant="h5" sx={{ fontWeight: 600 }}>
                📊 Distribution Overview
              </Typography>
            </DialogTitle>
            <DialogContent>
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '500px' }}>
                <ResponsiveContainer width="100%" height={450}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: 'Roles', value: governanceData.total_roles, color: '#1976d2' },
                        { name: 'Users', value: governanceData.total_users, color: '#00D4AA' },
                        { name: 'Protected Assets', value: governanceData.total_assets_with_rbac, color: '#f57c00' }
                      ]}
                      cx="50%"
                      cy="50%"
                      labelLine={true}
                      label={({ name, value, percent }) => `${name}: ${value} (${(percent * 100).toFixed(1)}%)`}
                      outerRadius={150}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {[{ name: 'Roles', value: governanceData.total_roles, color: '#1976d2' },
                        { name: 'Users', value: governanceData.total_users, color: '#00D4AA' },
                        { name: 'Protected Assets', value: governanceData.total_assets_with_rbac, color: '#f57c00' }].map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setDistributionDialogOpen(false)}>Close</Button>
            </DialogActions>
          </Dialog>

          {/* Roles Status Breakdown Dialog */}
          <Dialog
            open={rolesStatusDialogOpen}
            onClose={() => setRolesStatusDialogOpen(false)}
            maxWidth="md"
            fullWidth
          >
            <DialogTitle>
              <Typography variant="h5" sx={{ fontWeight: 600 }}>
                🔐 Roles Status Breakdown
              </Typography>
            </DialogTitle>
            <DialogContent>
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '500px' }}>
                <ResponsiveContainer width="100%" height={450}>
                  <PieChart>
                    <Pie
                      data={[
                        { 
                          name: 'Active Roles', 
                          value: governanceData.roles.filter(r => r.users.length > 0).length, 
                          color: '#4caf50' 
                        },
                        { 
                          name: 'Orphaned Roles', 
                          value: governanceData.roles.length - governanceData.roles.filter(r => r.users.length > 0).length, 
                          color: '#ff9800' 
                        }
                      ]}
                      cx="50%"
                      cy="50%"
                      labelLine={true}
                      label={({ name, value, percent }) => `${name}: ${value} (${(percent * 100).toFixed(1)}%)`}
                      outerRadius={150}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {[
                        { 
                          name: 'Active Roles', 
                          value: governanceData.roles.filter(r => r.users.length > 0).length, 
                          color: '#4caf50' 
                        },
                        { 
                          name: 'Orphaned Roles', 
                          value: governanceData.roles.length - governanceData.roles.filter(r => r.users.length > 0).length, 
                          color: '#ff9800' 
                        }
                      ].map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setRolesStatusDialogOpen(false)}>Close</Button>
            </DialogActions>
          </Dialog>

          {/* User Details Dialog */}
          <Dialog
            open={selectedUser !== null}
            onClose={() => setSelectedUser(null)}
            maxWidth="md"
            fullWidth
          >
            {selectedUser && governanceData && (
              <>
                <DialogTitle>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <Typography variant="h6" sx={{ fontWeight: 600 }}>
                      {selectedUser.username}
                    </Typography>
                  </Box>
                </DialogTitle>
                <DialogContent>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    <strong>User ID:</strong> {selectedUser.username}
                  </Typography>

                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Roles ({selectedUser.roles.length})
                  </Typography>
                  <Box sx={{ mb: 3 }}>
                    {selectedUser.roles.map((role, idx) => (
                      <Chip key={idx} label={role} color="primary" sx={{ m: 0.5 }} />
                    ))}
                  </Box>

                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Assets Accessible Through Roles
                  </Typography>
                  <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
                    <Table stickyHeader size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell sx={{ fontWeight: 600 }}>Asset</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Catalog</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Schema</TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Privileges</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {governanceData.assets_permissions
                          .filter(asset => {
                            // Check if user's roles overlap with asset's accessible roles
                            const userRoles = selectedUser.roles || [];
                            const assetRoles = asset.roles_with_access || [];
                            return userRoles.some(role => assetRoles.includes(role));
                          })
                          .map((asset, idx) => (
                            <TableRow key={idx}>
                              <TableCell>{asset.table}</TableCell>
                              <TableCell>{asset.catalog}</TableCell>
                              <TableCell>{asset.schema}</TableCell>
                              <TableCell>
                                {asset.privilege_type.map((priv, pidx) => (
                                  <Chip key={pidx} label={priv} size="small" sx={{ m: 0.25 }} />
                                ))}
                              </TableCell>
                            </TableRow>
                          ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 2, display: 'block' }}>
                    * Only showing assets where one or more of user's roles have access
                  </Typography>
                </DialogContent>
                <DialogActions>
                  <Button onClick={() => setSelectedUser(null)}>Close</Button>
                </DialogActions>
              </>
            )}
          </Dialog>
        </CardContent>
      </Card>
    </Box>
  );
};

export default TrinoGovernanceControlPage;

