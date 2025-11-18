<template>
  <div class="mb-3">
        <label for="sector" class="form-label">CI Sector:</label>
        <select
          id="sector"
          v-model="sector"
          class="form-select"
          required
        >
          <option
            value=""
            disabled
          >
            Select Sector
          </option>
          <option
            v-for="s in sectors"
            :key="s"
            :value="s"
          >
            {{ s }}
          </option>
        </select>
      </div>
  <div class="p-3 border rounded">
    
    <!-- bounding box info (if climate is loaded) -->
    <div
      v-if="climateBbox"
      class="alert alert-info"
    >
      Using climate bounding box:
      {{ climateBbox.min_lat }}, {{ climateBbox.max_lat }}
    </div>

    <!-- =============== A) User CSV/XLS (New Infrastructure) =============== -->
    <form @submit.prevent="onSubmitUserFile">
      <h5>Load Infrastructure (Custom CSV/XLS)</h5>
      <div class="mb-3">
        <label
          for="fileCustom"
          class="form-label"
        >Upload CSV/XLS:</label>
        <input
          id="fileCustom"
          type="file"
          class="form-control"
          accept=".csv, .xlsx"
          @change="onCustomFileSelected"
        >
      </div>

      <button
        type="submit"
        class="btn btn-success"
      >
        Load Infrastructure
      </button>
    </form>

    <hr>

    <!-- =============== B) AHA Data (Old Flow) =============== -->
    <form @submit.prevent="onSubmitAhaData">
      <img
        src="@/assets/AHACoreLogo.png"
        alt="AHACore Logo"
        class="modal-logo"
      >
      <h5>Load AHA Data</h5>

      <!-- optional: username/password if you want them -->
      <div class="mb-3">
        <label
          for="username"
          class="form-label"
        >Username:</label>
        <input
          id="username"
          v-model="username"
          type="text"
          class="form-control"
        >
      </div>

      <div class="mb-3">
        <label
          for="password"
          class="form-label"
        >Password:</label>
        <input
          id="password"
          v-model="password"
          type="password"
          class="form-control"
        >
      </div>

      <button
        type="submit"
        class="btn btn-primary"
      >
        Load AHA Data
      </button>
    </form>
  </div>
</template>

<script setup>

defineProps({
  
  
  
  climateBbox    : Object,                 // used in the template v-if
})

import { ref } from 'vue'
import { useDataStore } from '@/stores/data'
import { storeToRefs } from 'pinia'

const emit = defineEmits(['submit-infrastructure','file-upload'])


/* Pinia */
const store = useDataStore()
const { sector } = storeToRefs(store)           // two-way binding

/* dropdown */
const sectors = ['Energy Grid', 'Agriculture', 'Transportation']

/* user-file flow */
const selectedFile = ref(null)
function onCustomFileSelected(e){ selectedFile.value = e.target.files[0] }
function onSubmitUserFile(){
  if(!selectedFile.value) return alert('Choose a file first')
  emit('file-upload',{ file:selectedFile.value, sector:sector.value })
}

/* AHA flow */
const username = ref(''); const password = ref('')
function onSubmitAhaData(){
  emit('submit-infrastructure',{
    sector: sector.value, username:username.value, password:password.value
  })
}
</script>