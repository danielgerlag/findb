<template>
  <div class="app-layout">
    <aside class="app-sidebar">
      <h2>💰 FinanceDB</h2>
      <nav>
        <router-link to="/"><i class="pi pi-home"></i> Dashboard</router-link>
        <router-link to="/query"><i class="pi pi-code"></i> FQL Query</router-link>
        <router-link to="/accounts"><i class="pi pi-book"></i> Accounts</router-link>
        <router-link to="/journals"><i class="pi pi-file-edit"></i> Journals</router-link>
        <router-link to="/rates"><i class="pi pi-percentage"></i> Rates</router-link>
      </nav>
    </aside>
    <main class="app-main">
      <router-view />
    </main>
  </div>
  <Toast />
</template>

<script setup lang="ts">
import Toast from 'primevue/toast'
</script>
