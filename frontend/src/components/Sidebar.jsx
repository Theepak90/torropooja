import React, { useState } from 'react';
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Box,
  Collapse,
} from '@mui/material';
import {
  Dashboard,
  Storage,
  AccountTree,
  Publish,
  ExpandLess,
  ExpandMore,
  Search,
  Timeline,
  Security,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';

const Sidebar = ({ open, onClose }) => {
  const navigate = useNavigate();
  const location = useLocation();
  const [dataDiscoveryOpen, setDataDiscoveryOpen] = useState(false);

  const menuItems = [
    { label: 'Dashboard', icon: <Dashboard />, path: '/' },
    { 
      label: 'Data Discovery', 
      icon: <Search />, 
      path: null,
      children: [
        { label: 'Connectors', icon: <Storage />, path: '/connectors' },
        { label: 'Discovered Assets', icon: <AccountTree />, path: '/assets' },
        { label: 'Data Lineage', icon: <Timeline />, path: '/lineage' },
      ]
    },
    { label: 'Publish Assets to Marketplace', icon: <Publish />, path: '/marketplace' },
    { label: 'Trino Governance Control', icon: <Security />, path: '/governance' },
  ];

  const handleItemClick = (path) => {
    if (path) {
      navigate(path);
      onClose();
    }
  };

  const handleDataDiscoveryToggle = () => {
    setDataDiscoveryOpen(!dataDiscoveryOpen);
  };

  return (
    <Drawer
      anchor="left"
      open={open}
      onClose={onClose}
      sx={{
        '& .MuiDrawer-paper': {
          width: 280,
          backgroundColor: '#ffffff',
          borderRight: '1px solid #e5e7eb',
        },
      }}
    >
      <Box sx={{ pt: 3 }}>
        <List sx={{ px: 2 }}>
          {menuItems.map((item, index) => {
            const isActive = location.pathname === item.path;
            const hasChildren = item.children && item.children.length > 0;
            const isDataDiscoveryActive = hasChildren && item.children.some(child => location.pathname === child.path);

            if (hasChildren) {
              return (
                <React.Fragment key={item.label}>
                  <ListItem disablePadding>
                    <ListItemButton
                      onClick={handleDataDiscoveryToggle}
                      sx={{
                        borderRadius: 2,
                        mb: 0.5,
                        py: 1.5,
                        backgroundColor: isDataDiscoveryActive ? 'primary.main' : 'transparent',
                        color: isDataDiscoveryActive ? 'white' : 'text.primary',
                        '&:hover': {
                          backgroundColor: isDataDiscoveryActive ? 'primary.dark' : 'action.hover',
                        },
                      }}
                    >
                      <ListItemIcon sx={{ color: isDataDiscoveryActive ? 'white' : 'text.secondary', minWidth: 40 }}>
                        {item.icon}
                      </ListItemIcon>
                      <ListItemText 
                        primary={item.label}
                        sx={{
                          '& .MuiListItemText-primary': {
                            fontWeight: isDataDiscoveryActive ? 600 : 500,
                            fontSize: '0.95rem',
                          },
                        }}
                      />
                      {dataDiscoveryOpen ? <ExpandLess /> : <ExpandMore />}
                    </ListItemButton>
                  </ListItem>
                  <Collapse in={dataDiscoveryOpen} timeout="auto" unmountOnExit>
                    <List component="div" disablePadding>
                      {item.children.map((child) => {
                        const isChildActive = location.pathname === child.path;
                        return (
                          <ListItem key={child.path} disablePadding>
                            <ListItemButton
                              onClick={() => handleItemClick(child.path)}
                              sx={{
                                borderRadius: 2,
                                mb: 0.5,
                                py: 1.2,
                                pl: 4,
                                backgroundColor: isChildActive ? 'primary.main' : 'transparent',
                                color: isChildActive ? 'white' : 'text.primary',
                                '&:hover': {
                                  backgroundColor: isChildActive ? 'primary.dark' : 'action.hover',
                                },
                              }}
                            >
                              <ListItemIcon sx={{ color: isChildActive ? 'white' : 'text.secondary', minWidth: 40 }}>
                                {child.icon}
                              </ListItemIcon>
                              <ListItemText 
                                primary={child.label}
                                sx={{
                                  '& .MuiListItemText-primary': {
                                    fontWeight: isChildActive ? 600 : 500,
                                    fontSize: '0.9rem',
                                  },
                                }}
                              />
                            </ListItemButton>
                          </ListItem>
                        );
                      })}
                    </List>
                  </Collapse>
                </React.Fragment>
              );
            }

            return (
              <ListItem key={item.path} disablePadding>
                <ListItemButton
                  onClick={() => handleItemClick(item.path)}
                  sx={{
                    borderRadius: 2,
                    mb: 0.5,
                    py: 1.5,
                    backgroundColor: isActive ? 'primary.main' : 'transparent',
                    color: isActive ? 'white' : 'text.primary',
                    '&:hover': {
                      backgroundColor: isActive ? 'primary.dark' : 'action.hover',
                    },
                  }}
                >
                  <ListItemIcon sx={{ color: isActive ? 'white' : 'text.secondary', minWidth: 40 }}>
                    {item.icon}
                  </ListItemIcon>
                  <ListItemText 
                    primary={item.label}
                    sx={{
                      '& .MuiListItemText-primary': {
                        fontWeight: isActive ? 600 : 500,
                        fontSize: '0.95rem',
                      },
                    }}
                  />
                </ListItemButton>
              </ListItem>
            );
          })}
        </List>
      </Box>
    </Drawer>
  );
};

export default Sidebar;
