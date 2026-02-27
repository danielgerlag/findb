import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from './views/DashboardView.vue'
import QueryView from './views/QueryView.vue'
import AccountsView from './views/AccountsView.vue'
import JournalsView from './views/JournalsView.vue'
import RatesView from './views/RatesView.vue'
import TourView from './views/TourView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: DashboardView },
    { path: '/query', component: QueryView },
    { path: '/tour', component: TourView },
    { path: '/accounts', component: AccountsView },
    { path: '/journals', component: JournalsView },
    { path: '/rates', component: RatesView },
  ],
})

export default router
