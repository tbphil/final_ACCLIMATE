import { Routes } from '@angular/router';
import { Layout } from './components/layout/layout';
import { Stepper } from './components/stepper/stepper';
import { Home } from './components/home/home';

export const routes: Routes = [
  {
    path: '',
    component: Layout,
    children: [
      { path: 'home', component: Home },

      { path: '', pathMatch: 'full', redirectTo: 'home' }, // Default route
    ],
  },
];
